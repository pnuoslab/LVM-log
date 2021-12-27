/*
 * Copyright (C) 2001-2003 Sistina Software (UK) Limited.
 *
 * This file is released under the GPL.
 */

#include "dm.h"
#include <linux/device-mapper.h>

#include <linux/module.h>
#include <linux/init.h>
#include <linux/blkdev.h>
#include <linux/bio.h>
#include <linux/dax.h>
#include <linux/slab.h>
#include <linux/log2.h>

#include <linux/cgroup.h>
#include <linux/rbtree.h>

/* TODO: Refactoring */
#include "../../block/blk-stat.h"

#define DM_MSG_PREFIX "striped"
#define DM_IO_ERROR_THRESHOLD 15

/* Do GC n sec after last dispatched */
#define DM_GC_TIME		5 * NSEC_PER_SEC

/* Every n sec, get current BW or latency */
#define DM_MONITOR_PERIOD	1 * MSEC_PER_SEC

#define MAXDEVS			5
#define LOGBLOCK_SHIFT		3

struct logblock {
	/* Original addr. */
	sector_t origin;

	/* Remapped addr. */
	sector_t mapped;

	uint32_t dev;

	struct rb_node node;
	struct llist_node list;

	/* GC info */
	struct bio *bio;
	struct stripe_c *sc;
};

struct stripe_log {
	struct block_device *bdev;

	/* Available log block list */
	struct llist_head lb;

	/* List Lock */
	spinlock_t lock;

	/* Latency Monitor */
	struct blk_stat_callback *cb;
	atomic64_t mean;

	/* For kfree */
	struct logblock *lbs;
};
static struct stripe_log sl[MAXDEVS];

struct stripe_gc {
	/* GC structures */
	struct hrtimer timer;

	/* GC flags */
	unsigned long flags;

	/* Reads from a dev and copies it to the another when GC. */
	struct workqueue_struct *gc_readqueue;
	struct work_struct readwork;
	struct llist_head gc_readlist;

	struct workqueue_struct *gc_writequeue;
	struct work_struct writework;
	struct llist_head gc_writelist;
};
static struct stripe_gc sg;

/* Number of stripes */
static unsigned int devs = 0;

struct stripe {
	struct dm_dev *dev;
	sector_t physical_start;

	atomic_t error_count;
};

struct stripe_c {
	uint32_t stripes;
	int stripes_shift;

	/* The size of this target / num. stripes */
	sector_t stripe_width;

	uint32_t chunk_size;
	int chunk_size_shift;

	/* Needed for handling events */
	struct dm_target *ti;

	/* Work struct used for triggering events*/
	struct work_struct trigger_event;

	/* If addr. is remapped, log block is in this tree */
	struct rb_root tree;
	spinlock_t lock;

	/* Must be the last member */
	struct stripe stripe[0];
};

/* GC flags */
enum {
	DM_GC_DTR,	/* Doing destroy */
	DM_FLAGS	/* # of flags */
};

static inline bool gc_flagged(struct stripe_gc *sg, unsigned int bit)
{
	return test_bit(bit, &sg->flags);
}

static inline void gc_set_flag(struct stripe_gc *sg, unsigned int bit)
{
	set_bit(bit, &sg->flags);
}

static inline void gc_clear_flag(struct stripe_gc *sg, unsigned int bit)
{
	clear_bit(bit, &sg->flags);
}

/*
 * An event is triggered whenever a drive
 * drops out of a stripe volume.
 */
static void trigger_event(struct work_struct *work)
{
	struct stripe_c *sc = container_of(work, struct stripe_c,
					   trigger_event);
	dm_table_event(sc->ti->table);
}

static inline struct stripe_c *alloc_context(unsigned int stripes)
{
	size_t len;

	if (dm_array_too_big(sizeof(struct stripe_c), sizeof(struct stripe),
			     stripes))
		return NULL;

	len = sizeof(struct stripe_c) + (sizeof(struct stripe) * stripes);

	return kmalloc(len, GFP_KERNEL);
}

/*
 * Pre-allocate log blocks.
 */

static int get_lbs(struct stripe_c *sc, sector_t nr_sects)
{
	sector_t chunk = nr_sects;
	sector_t nr_lbs = chunk;
	struct logblock *lbs;
	unsigned int i;

	if (sc->chunk_size_shift < 0)
		sector_div(nr_lbs, sc->chunk_size);
	else
		nr_lbs >>= sc->chunk_size_shift;

	nr_lbs = (nr_lbs >> LOGBLOCK_SHIFT) - 1;

	lbs = kvmalloc_array(nr_lbs, sizeof(struct logblock), GFP_KERNEL);
	if (!lbs)
		return -ENOMEM;

	sl[devs].lbs = lbs;

	if (sc->chunk_size_shift < 0)
		sector_div(chunk, sc->chunk_size);
	else
		chunk >>= sc->chunk_size_shift;

	chunk = chunk - nr_lbs - 1;
	for (i = 0; i < nr_lbs; i++) {
		lbs[i].mapped = chunk + i;
		lbs[i].dev = devs;

		llist_add(&lbs[i].list, &sl[devs].lb);
	}

	return 0;
}

static void lm_timer_fn(struct blk_stat_callback *cb)
{
	struct stripe_log *sl = cb->data;

	atomic64_set(&sl->mean, cb->stat->mean);

	blk_stat_activate_msecs(cb, DM_MONITOR_PERIOD);
}

static int lm_data_dir(const struct request *rq)
{
	const int op = req_op(rq);

	/* don't account */
	if (!op_is_write(op))
		return -1;

	return 0;
}

/* Latency Monitor */
static int get_lm(struct request_queue *q)
{
	sl[devs].cb = blk_stat_alloc_callback(lm_timer_fn, lm_data_dir,
						1, &sl[devs]);
	if (!sl[devs].cb)
		return -ENOMEM;

	atomic64_set(&sl[devs].mean, 0);
	
	blk_stat_add_callback(q, sl[devs].cb);
	blk_stat_activate_msecs(sl[devs].cb, DM_MONITOR_PERIOD);

	return 0;
}

static uint32_t chunk_dev(struct stripe_c *sc, sector_t chunk)
{
	if (sc->stripes_shift < 0)
		return sector_div(chunk, sc->stripes);
	else
		return chunk & (sc->stripes - 1);
}

static void stripe_map_sector(struct stripe_c *sc, sector_t sector,
		uint32_t *stripe, sector_t *result);

static void gc_write_endio(struct bio *bio)
{
	bio_free_pages(bio);
	bio_put(bio);
}

static void do_gc_write(struct work_struct *work)
{
	struct stripe_c *sc;
	struct bio *bio;
	struct llist_node *lnode;
	struct logblock *lb;
	sector_t chunk;
	unsigned int stripe;

	for (;;) {
		lnode = llist_del_first(&sg.gc_writelist);
		if (!lnode)
			break;

		lb = llist_entry(lnode, struct logblock, list);
		sc = lb->sc;

		chunk = lb->origin;

		if (sc->stripes_shift < 0)
			chunk *= sc->chunk_size;
		else
			chunk <<= sc->chunk_size_shift;

		bio = lb->bio;

		memset(&bio->bi_iter, 0, sizeof(struct bvec_iter));

		bio->bi_iter.bi_sector = chunk + sc->ti->begin;
		bio->bi_iter.bi_size = sc->chunk_size * SECTOR_SIZE;

		bio->bi_end_io = gc_write_endio;
		bio->bi_opf = REQ_OP_WRITE;

		stripe_map_sector(sc, bio->bi_iter.bi_sector,
				&stripe, &bio->bi_iter.bi_sector);

		bio->bi_iter.bi_sector += sc->stripe[stripe].physical_start;
		bio_set_dev(bio, sc->stripe[stripe].dev->bdev);

		/* Make a request to the original addr. */
		generic_make_request(bio);

		spin_lock(&sc->lock);
		rb_erase(&lb->node, &sc->tree);
		spin_unlock(&sc->lock);

		if (!gc_flagged(&sg, DM_GC_DTR))
			llist_add(&lb->list, &sl[lb->dev].lb);
	}
}

static void gc_read_endio(struct bio *bio)
{
	struct logblock *lb = bio->bi_private;

	llist_add(&lb->list, &sg.gc_writelist);
	queue_work(sg.gc_writequeue, &sg.writework);
}

static struct bio *make_gc_bio(struct logblock *lb)
{
	struct bio *bio;
	struct page *page;
	struct stripe_c *sc = lb->sc;
	sector_t chunk = dm_target_offset(sc->ti, lb->mapped);
	unsigned int len;
	unsigned int stripe;

	len = DIV_ROUND_UP(sc->chunk_size * SECTOR_SIZE, PAGE_SIZE);

	bio = bio_kmalloc(GFP_KERNEL, len);
	if (!bio)
		goto err_bio;

	for (; len != 0; len--) {
		page = alloc_page(GFP_KERNEL);
		if (!page)
			goto err_page;
		bio_add_page(bio, page, PAGE_SIZE, 0);
	}

	if (sc->stripes_shift < 0)
		chunk *= sc->chunk_size;
	else
		chunk <<= sc->chunk_size_shift;

	memset(&bio->bi_iter, 0, sizeof(struct bvec_iter));

	bio->bi_iter.bi_sector = chunk + sc->ti->begin;
	bio->bi_iter.bi_size = sc->chunk_size * SECTOR_SIZE;

	bio->bi_end_io = gc_read_endio;
	bio->bi_opf = REQ_OP_READ;

	stripe_map_sector(sc, bio->bi_iter.bi_sector,
			&stripe, &bio->bi_iter.bi_sector);

	bio->bi_iter.bi_sector += sc->stripe[stripe].physical_start;
	bio_set_dev(bio, sc->stripe[stripe].dev->bdev);

	return bio;
err_page:
	bio_free_pages(bio);
	bio_put(bio);
err_bio:
	return NULL;
}

static void do_gc_read(struct work_struct *work)
{
	struct logblock *lb;
	struct bio *bio;
	struct llist_node *lnode;

	for (;;) {
		lnode = llist_del_first(&sg.gc_readlist);
		if (!lnode)
			break;

		lb = llist_entry(lnode, struct logblock, list);

		bio = make_gc_bio(lb);
		if (!bio) {
			DMERR("Can't allocate bio");
			llist_add(&lb->list, &sg.gc_readlist);
		}
		bio->bi_private = (void *) lb;
		lb->bio = bio;

		generic_make_request(bio);
	}
}

static enum hrtimer_restart stripe_do_gc(struct hrtimer *timer)
{
	queue_work(sg.gc_readqueue, &sg.readwork);

	return HRTIMER_NORESTART;
}

static int get_gc(struct stripe_c *sc)
{
	int ret = 0;

	bitmap_zero(&sg.flags, DM_FLAGS);

	hrtimer_init(&sg.timer, CLOCK_MONOTONIC, HRTIMER_MODE_REL);
	sg.timer.function = stripe_do_gc;

	sg.gc_readqueue = alloc_workqueue("kdmgcread", WQ_MEM_RECLAIM, 0);
	if (!sg.gc_readqueue) {
		sc->ti->error = "Couldn't allocate workqueue";
		ret = -ENOMEM;
		goto err_gc_readqueue;
	}
	INIT_WORK(&sg.readwork, do_gc_read);
	init_llist_head(&sg.gc_readlist);

	sg.gc_writequeue = alloc_workqueue("kdmgcwrite", WQ_MEM_RECLAIM, 0);
	if (!sg.gc_writequeue) {
		sc->ti->error = "Couldn't allocate workqueue";
		ret = -ENOMEM;
		goto err_gc_writequeue;
	}
	INIT_WORK(&sg.writework, do_gc_write);
	init_llist_head(&sg.gc_writelist);

	return 0;

err_gc_writequeue:
	destroy_workqueue(sg.gc_readqueue);
err_gc_readqueue:
	hrtimer_cancel(&sg.timer);

	return ret;
}

static int get_sl(struct stripe_c *sc, struct dm_dev *dev)
{
	int ret;
	u64 i;

	for (i = 0; i<MAXDEVS; i++)
		if (sl[i].bdev == dev->bdev)
			return 0;

	sl[devs].bdev = dev->bdev;

	/* Initalize available log sector list and lock */
	init_llist_head(&sl[devs].lb);
	spin_lock_init(&sl[devs].lock);

	ret = get_lbs(sc, dev->bdev->bd_part->nr_sects);
	if (ret)
		goto err_lbs;

	ret = get_lm(dev->bdev->bd_queue);
	if (ret)
		goto err_lm;

	if (!devs) {
		ret = get_gc(sc);
		if (ret)
			goto err_gc;
	}

	devs++;

	return 0;
err_gc:
	blk_stat_remove_callback(sl[devs].bdev->bd_queue, sl[devs].cb);
	blk_stat_free_callback(sl[devs].cb);
err_lm:
	kvfree(sl[devs].lbs);
err_lbs:
	dm_put_device(sc->ti, dev);

	return ret;
}

/*
 * Parse a single <dev> <sector> pair
 */
static int get_stripe(struct dm_target *ti, struct stripe_c *sc,
		unsigned int stripe, char **argv)
{
	unsigned long long start;
	char dummy;
	int ret;

	if (sscanf(argv[1], "%llu%c", &start, &dummy) != 1)
		return -EINVAL;

	ret = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table),
			&sc->stripe[stripe].dev);
	if (ret)
		return ret;

	sc->stripe[stripe].physical_start = start;

	return get_sl(sc, sc->stripe[stripe].dev);
}

/*
 * Construct a striped mapping.
 * <number of stripes> <chunk size> [<dev_path> <offset>]+
 */
static int stripe_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct stripe_c *sc;
	sector_t width, tmp_len;
	uint32_t stripes;
	uint32_t chunk_size;
	int r;
	unsigned int i;

	if (argc < 2) {
		ti->error = "Not enough arguments";
		return -EINVAL;
	}

	if (kstrtouint(argv[0], 10, &stripes) || !stripes) {
		ti->error = "Invalid stripe count";
		return -EINVAL;
	}

	if (kstrtouint(argv[1], 10, &chunk_size) || !chunk_size) {
		ti->error = "Invalid chunk_size";
		return -EINVAL;
	}

	width = ti->len;
	if (sector_div(width, stripes)) {
		ti->error = "Target length not divisible by "
			"number of stripes";
		return -EINVAL;
	}

	tmp_len = width;
	if (sector_div(tmp_len, chunk_size)) {
		ti->error = "Target length not divisible by "
			"chunk size";
		return -EINVAL;
	}

	/*
	 * Do we have enough arguments for that many stripes ?
	 */
	if (argc != (2 + 2 * stripes)) {
		ti->error = "Not enough destinations "
			"specified";
		return -EINVAL;
	}

	sc = alloc_context(stripes);
	if (!sc) {
		ti->error = "Memory allocation for striped context "
			"failed";
		return -ENOMEM;
	}

	INIT_WORK(&sc->trigger_event, trigger_event);

	/* Set pointer to dm target; used in trigger_event */
	sc->ti = ti;
	sc->stripes = stripes;
	sc->stripe_width = width;

	if (stripes & (stripes - 1))
		sc->stripes_shift = -1;
	else
		sc->stripes_shift = __ffs(stripes);

	r = dm_set_target_max_io_len(ti, chunk_size);
	if (r)
		goto err_stripe;

	ti->num_flush_bios = stripes;
	ti->num_discard_bios = stripes;
	ti->num_secure_erase_bios = stripes;
	ti->num_write_same_bios = stripes;
	ti->num_write_zeroes_bios = stripes;

	sc->chunk_size = chunk_size;
	if (chunk_size & (chunk_size - 1))
		sc->chunk_size_shift = -1;
	else
		sc->chunk_size_shift = __ffs(chunk_size);

	/*
	 * Get the stripe destinations.
	 */
	for (i = 0; i < stripes; i++) {
		argv += 2;

		r = get_stripe(ti, sc, i, argv);
		if (r < 0) {
			ti->error = "Couldn't parse stripe destination";
			while (i--)
				dm_put_device(ti, sc->stripe[i].dev);
			return r;
		}
		atomic_set(&(sc->stripe[i].error_count), 0);
	}

	sc->tree = RB_ROOT;
	spin_lock_init(&sc->lock);

	ti->private = sc;

	return 0;
err_stripe:
	kfree(sc);
	return r;
}

static void stripe_dtr(struct dm_target *ti)
{
	unsigned int i;
	struct stripe_c *sc = (struct stripe_c *) ti->private;

	for (i = 0; i < sc->stripes; i++)
		dm_put_device(ti, sc->stripe[i].dev);

	flush_work(&sc->trigger_event);

	kfree(sc);
}

static void stripe_map_sector(struct stripe_c *sc, sector_t sector,
		uint32_t *stripe, sector_t *result)
{
	sector_t chunk = dm_target_offset(sc->ti, sector);
	sector_t chunk_offset;

	if (sc->chunk_size_shift < 0)
		chunk_offset = sector_div(chunk, sc->chunk_size);
	else {
		chunk_offset = chunk & (sc->chunk_size - 1);
		chunk >>= sc->chunk_size_shift;
	}

	if (sc->stripes_shift < 0)
		*stripe = sector_div(chunk, sc->stripes);
	else {
		*stripe = chunk & (sc->stripes - 1);
		chunk >>= sc->stripes_shift;
	}

	if (sc->chunk_size_shift < 0)
		chunk *= sc->chunk_size;
	else
		chunk <<= sc->chunk_size_shift;

	*result = chunk + chunk_offset;
}

static void stripe_map_range_sector(struct stripe_c *sc, sector_t sector,
		uint32_t target_stripe, sector_t *result)
{
	uint32_t stripe;

	stripe_map_sector(sc, sector, &stripe, result);
	if (stripe == target_stripe)
		return;

	/* round down */
	sector = *result;
	if (sc->chunk_size_shift < 0)
		*result -= sector_div(sector, sc->chunk_size);
	else
		*result = sector & ~(sector_t)(sc->chunk_size - 1);

	if (target_stripe < stripe)
		*result += sc->chunk_size;		/* next chunk */
}

static int stripe_map_range(struct stripe_c *sc, struct bio *bio,
		uint32_t target_stripe)
{
	sector_t begin, end;

	stripe_map_range_sector(sc, bio->bi_iter.bi_sector,
			target_stripe, &begin);
	stripe_map_range_sector(sc, bio_end_sector(bio),
			target_stripe, &end);
	if (begin < end) {
		bio_set_dev(bio, sc->stripe[target_stripe].dev->bdev);
		bio->bi_iter.bi_sector = begin +
			sc->stripe[target_stripe].physical_start;
		bio->bi_iter.bi_size = to_bytes(end - begin);
		return DM_MAPIO_REMAPPED;
	} else {
		/* The range doesn't map to the target stripe */
		bio_endio(bio);
		return DM_MAPIO_SUBMITTED;
	}
}

static int rb_insert(struct rb_root *root, struct logblock *new)
{
	struct rb_node **link = &(root->rb_node), *parent = NULL;
	sector_t val = new->origin;
	struct logblock *lb;

	while (*link) {
		lb = rb_entry(*link, struct logblock, node);

		parent = *link;
		if (val < lb->origin)
			link = &((*link)->rb_left);
		else if (val > lb->origin)
			link = &((*link)->rb_right);
		else
			return -EINVAL;
	}

	rb_link_node(&new->node, parent, link);
	rb_insert_color(&new->node, root);

	return 0;
}

static struct logblock *rb_search(struct rb_root *root, sector_t val)
{
	struct rb_node *node = root->rb_node;
	struct logblock *lb = NULL;

	while (node) {
		lb = rb_entry(node, struct logblock, node);

		if (val < lb->origin)
			node = node->rb_left;
		else if (val > lb->origin)
			node = node->rb_right;
		else
			return lb;
	}

	return NULL;
}

static struct logblock *stripe_getlb(struct stripe_c *sc, uint32_t ndev)
{
	struct llist_node *lnode;
	struct block_device *bdev = sc->stripe[ndev].dev->bdev;
	unsigned int i;

	for (i = 0; i < devs; i++)
		if (bdev == sl[i].bdev)
			break;

	if (i == devs)
		return NULL;

	spin_lock(&sl[i].lock);
	lnode = llist_del_first(&sl[i].lb);
	spin_unlock(&sl[i].lock);

	return lnode ? llist_entry(lnode, struct logblock, list) : NULL;
}

static int32_t stripe_dev_reassign(struct stripe_c *sc, struct bio *bio,
		unsigned int weight)
{
	u64 min_lat = U64_MAX;
	unsigned int ndev = -1, i;

	if (weight != atomic_read(&bio->bi_disk->queue->max_weight))
		return -1;

	for (i = 0; i < sc->stripes; i++) {
		if (sl[i].cb->stat->nr_samples == 0)
			return -1;

		if (min_lat > sl[i].cb->stat->mean) {
			ndev = i;
			min_lat = sl[i].cb->stat->mean;
		}
	}

	return ndev;
}

static bool stripe_check_and_assign(struct stripe_c *sc, struct bio *bio)
{
	unsigned int weight;
	struct logblock *lb = NULL, temp;
	sector_t origin = dm_target_offset(sc->ti, bio->bi_iter.bi_sector);
	sector_t chunk_offset;
	sector_t mapped;
	int32_t odev, ndev;
	bool reassigned = false;

	if (sc->chunk_size_shift < 0)
		chunk_offset = sector_div(origin, sc->chunk_size);
	else {
		chunk_offset = origin & (sc->chunk_size - 1);
		origin >>= sc->chunk_size_shift;
	}
	mapped = origin;

	/*
	 * If lb isn't null, this addr. is already mapped
	 * Else, prepare to insert node
	 */
	spin_lock(&sc->lock);
	lb = rb_search(&sc->tree, origin);
	if (lb) {
		if (lb->sc)
			mapped = lb->mapped;
		spin_unlock(&sc->lock);
		reassigned = true;
		goto out;
	} else {
		if (!op_is_write(bio_op(bio))) {
			spin_unlock(&sc->lock);
			goto out;
		}
		temp.origin = origin;
		temp.sc = NULL;
		rb_insert(&sc->tree, &temp);
	}
	spin_unlock(&sc->lock);

	odev = chunk_dev(sc, origin);

	weight = bio_blkcg(bio) ? bio_blkcg(bio)->weight : 0;
	ndev = stripe_dev_reassign(sc, bio, weight);

	if (ndev < 0 || ndev == odev) {
		spin_lock(&sc->lock);
		rb_erase(&temp.node, &sc->tree);
		spin_unlock(&sc->lock);
		goto out;
	}

	lb = stripe_getlb(sc, ndev);
	if (!lb) {
		spin_lock(&sc->lock);
		rb_erase(&temp.node, &sc->tree);
		spin_unlock(&sc->lock);
		goto out;
	}

	lb->origin = origin;
	lb->sc = sc;

	spin_lock(&sc->lock);
	rb_replace_node(&temp.node, &lb->node, &sc->tree);
	spin_unlock(&sc->lock);

	llist_add(&lb->list, &sg.gc_readlist);

	mapped = lb->mapped;
	reassigned = true;
out:
	if (sc->chunk_size_shift < 0)
		mapped *= sc->chunk_size;
	else
		mapped <<= sc->chunk_size_shift;

	bio->bi_iter.bi_sector = mapped + chunk_offset + sc->ti->begin;

	return reassigned;
}

static int stripe_map(struct dm_target *ti, struct bio *bio)
{
	struct stripe_c *sc = ti->private;
	uint32_t stripe;
	unsigned target_bio_nr;
	bool reassigned = false;

	reassigned = stripe_check_and_assign(sc, bio);

	if (bio->bi_opf & REQ_PREFLUSH) {
		target_bio_nr = dm_bio_get_target_bio_nr(bio);
		BUG_ON(target_bio_nr >= sc->stripes);
		bio_set_dev(bio, sc->stripe[target_bio_nr].dev->bdev);
		return DM_MAPIO_REMAPPED;
	}
	if (unlikely(bio_op(bio) == REQ_OP_DISCARD) ||
			unlikely(bio_op(bio) == REQ_OP_SECURE_ERASE) ||
			unlikely(bio_op(bio) == REQ_OP_WRITE_ZEROES) ||
			unlikely(bio_op(bio) == REQ_OP_WRITE_SAME)) {
		target_bio_nr = dm_bio_get_target_bio_nr(bio);
		BUG_ON(target_bio_nr >= sc->stripes);
		return stripe_map_range(sc, bio, target_bio_nr);
	}

	stripe_map_sector(sc, bio->bi_iter.bi_sector,
			&stripe, &bio->bi_iter.bi_sector);

	if (!reassigned)
		bio->bi_iter.bi_sector += sc->stripe[stripe].physical_start;
	bio_set_dev(bio, sc->stripe[stripe].dev->bdev);

	return DM_MAPIO_REMAPPED;
}

#if IS_ENABLED(CONFIG_DAX_DRIVER)
static long stripe_dax_direct_access(struct dm_target *ti, pgoff_t pgoff,
		long nr_pages, void **kaddr, pfn_t *pfn)
{
	sector_t dev_sector, sector = pgoff * PAGE_SECTORS;
	struct stripe_c *sc = ti->private;
	struct dax_device *dax_dev;
	struct block_device *bdev;
	uint32_t stripe;
	long ret;

	stripe_map_sector(sc, sector, &stripe, &dev_sector);
	dev_sector += sc->stripe[stripe].physical_start;
	dax_dev = sc->stripe[stripe].dev->dax_dev;
	bdev = sc->stripe[stripe].dev->bdev;

	ret = bdev_dax_pgoff(bdev, dev_sector, nr_pages * PAGE_SIZE, &pgoff);
	if (ret)
		return ret;
	return dax_direct_access(dax_dev, pgoff, nr_pages, kaddr, pfn);
}

static size_t stripe_dax_copy_from_iter(struct dm_target *ti, pgoff_t pgoff,
		void *addr, size_t bytes, struct iov_iter *i)
{
	sector_t dev_sector, sector = pgoff * PAGE_SECTORS;
	struct stripe_c *sc = ti->private;
	struct dax_device *dax_dev;
	struct block_device *bdev;
	uint32_t stripe;

	stripe_map_sector(sc, sector, &stripe, &dev_sector);
	dev_sector += sc->stripe[stripe].physical_start;
	dax_dev = sc->stripe[stripe].dev->dax_dev;
	bdev = sc->stripe[stripe].dev->bdev;

	if (bdev_dax_pgoff(bdev, dev_sector, ALIGN(bytes, PAGE_SIZE), &pgoff))
		return 0;
	return dax_copy_from_iter(dax_dev, pgoff, addr, bytes, i);
}

static size_t stripe_dax_copy_to_iter(struct dm_target *ti, pgoff_t pgoff,
		void *addr, size_t bytes, struct iov_iter *i)
{
	sector_t dev_sector, sector = pgoff * PAGE_SECTORS;
	struct stripe_c *sc = ti->private;
	struct dax_device *dax_dev;
	struct block_device *bdev;
	uint32_t stripe;

	stripe_map_sector(sc, sector, &stripe, &dev_sector);
	dev_sector += sc->stripe[stripe].physical_start;
	dax_dev = sc->stripe[stripe].dev->dax_dev;
	bdev = sc->stripe[stripe].dev->bdev;

	if (bdev_dax_pgoff(bdev, dev_sector, ALIGN(bytes, PAGE_SIZE), &pgoff))
		return 0;
	return dax_copy_to_iter(dax_dev, pgoff, addr, bytes, i);
}

#else
#define stripe_dax_direct_access NULL
#define stripe_dax_copy_from_iter NULL
#define stripe_dax_copy_to_iter NULL
#endif

/*
 * Stripe status:
 *
 * INFO
 * #stripes [stripe_name <stripe_name>] [group word count]
 * [error count 'A|D' <error count 'A|D'>]
 *
 * TABLE
 * #stripes [stripe chunk size]
 * [stripe_name physical_start <stripe_name physical_start>]
 *
 */

static void stripe_status(struct dm_target *ti, status_type_t type,
		unsigned status_flags, char *result, unsigned maxlen)
{
	struct stripe_c *sc = (struct stripe_c *) ti->private;
	unsigned int sz = 0;
	unsigned int i;

	switch (type) {
		case STATUSTYPE_INFO:
			DMEMIT("%d ", sc->stripes);
			for (i = 0; i < sc->stripes; i++)  {
				DMEMIT("%s ", sc->stripe[i].dev->name);
			}
			DMEMIT("1 ");
			for (i = 0; i < sc->stripes; i++) {
				DMEMIT("%c", atomic_read(&(sc->stripe[i].error_count)) ?
						'D' : 'A');
			}
			break;

		case STATUSTYPE_TABLE:
			DMEMIT("%d %llu", sc->stripes,
					(unsigned long long)sc->chunk_size);
			for (i = 0; i < sc->stripes; i++)
				DMEMIT(" %s %llu", sc->stripe[i].dev->name,
						(unsigned long long)sc->stripe[i].physical_start);
			break;
	}
}

static int stripe_end_io(struct dm_target *ti, struct bio *bio,
		blk_status_t *error)
{
	unsigned i;
	char major_minor[16];
	struct stripe_c *sc = ti->private;

	hrtimer_start(&sg.timer, DM_GC_TIME, HRTIMER_MODE_REL);

	if (!*error)
		return DM_ENDIO_DONE; /* I/O complete */

	if (bio->bi_opf & REQ_RAHEAD)
		return DM_ENDIO_DONE;

	if (*error == BLK_STS_NOTSUPP)
		return DM_ENDIO_DONE;

	memset(major_minor, 0, sizeof(major_minor));
	sprintf(major_minor, "%d:%d", MAJOR(bio_dev(bio)), MINOR(bio_dev(bio)));

	/*
	 * Test to see which stripe drive triggered the event
	 * and increment error count for all stripes on that device.
	 * If the error count for a given device exceeds the threshold
	 * value we will no longer trigger any further events.
	 */
	for (i = 0; i < sc->stripes; i++)
		if (!strcmp(sc->stripe[i].dev->name, major_minor)) {
			atomic_inc(&(sc->stripe[i].error_count));
			if (atomic_read(&(sc->stripe[i].error_count)) <
					DM_IO_ERROR_THRESHOLD)
				schedule_work(&sc->trigger_event);
		}

	return DM_ENDIO_DONE;
}

static int stripe_iterate_devices(struct dm_target *ti,
		iterate_devices_callout_fn fn, void *data)
{
	struct stripe_c *sc = ti->private;
	int ret = 0;
	unsigned i = 0;

	do {
		ret = fn(ti, sc->stripe[i].dev,
				sc->stripe[i].physical_start,
				sc->stripe_width, data);
	} while (!ret && ++i < sc->stripes);

	return ret;
}

static void stripe_io_hints(struct dm_target *ti,
		struct queue_limits *limits)
{
	struct stripe_c *sc = ti->private;
	unsigned chunk_size = sc->chunk_size << SECTOR_SHIFT;

	blk_limits_io_min(limits, chunk_size);
	blk_limits_io_opt(limits, chunk_size * sc->stripes);
}

static struct target_type stripe_target = {
	.name   = "striped",
	.version = {1, 6, 0},
	.features = DM_TARGET_PASSES_INTEGRITY,
	.module = THIS_MODULE,
	.ctr    = stripe_ctr,
	.dtr    = stripe_dtr,
	.map    = stripe_map,
	.end_io = stripe_end_io,
	.status = stripe_status,
	.iterate_devices = stripe_iterate_devices,
	.io_hints = stripe_io_hints,
	.direct_access = stripe_dax_direct_access,
	.dax_copy_from_iter = stripe_dax_copy_from_iter,
	.dax_copy_to_iter = stripe_dax_copy_to_iter,
};

int __init dm_stripe_init(void)
{
	int r;

	r = dm_register_target(&stripe_target);
	if (r < 0)
		DMWARN("target registration failed");

	return r;
}

void dm_stripe_exit(void)
{
	unsigned int i;

	for (i = 0; i < devs; i++) {
		kvfree(sl[i].lbs);
		blk_stat_remove_callback(sl[i].bdev->bd_queue, sl[i].cb);
		blk_stat_free_callback(sl[i].cb);

		hrtimer_cancel(&sg.timer);

		/* Flush gc_queue */
		gc_set_flag(&sg, DM_GC_DTR);

		queue_work(sg.gc_readqueue, &sg.readwork);
		drain_workqueue(sg.gc_readqueue);
		destroy_workqueue(sg.gc_readqueue);

		while (!llist_empty(&sg.gc_readlist) ||
				!llist_empty(&sg.gc_writelist))
			cpu_relax();

		destroy_workqueue(sg.gc_writequeue);
	}

	dm_unregister_target(&stripe_target);
}
