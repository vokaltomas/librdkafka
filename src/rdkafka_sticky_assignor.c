/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2020 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "rdkafka_int.h"
#include "rdkafka_assignor.h"
#include "rdmap.h"

#include <math.h>  /* abs() */

/**
 * @name KIP-54 and KIP-341 Sticky assignor.
 *
 * Closely mimicking the official Apache Kafka AbstractStickyAssignor
 * implementation.
 */


/** FIXME
 * Remaining:

isBalanced()
isSticky()

 */

/**
 * Auxilliary glue types
 */

/**
 * @struct ConsumerPair_t represents a pair of consumer member ids involved in
 *         a partition reassignment, indicating a source consumer a partition
 *         is moving from and a destination partition the same partition is
 *         moving to.
 *
 * @sa PartitionMovements_t
 */
typedef struct ConsumerPair_s {
        const char *src;  /**< Source member id */
        const char *dst;  /**< Destination member id */
} ConsumerPair_t;


static ConsumerPair_t *ConsumerPair_new (const char *src, const char *dst) {
        ConsumerPair_t *cpair;

        cpair = rd_malloc(sizeof(*cpair));
        cpair->src = src ? rd_strdup(src) : NULL;
        cpair->dst = dst ? rd_strdup(dst) : NULL;

        return cpair;
}


static void ConsumerPair_free (void *p) {
        ConsumerPair_t *cpair = p;
        if (cpair->src)
                rd_free((void *)cpair->src);
        if (cpair->dst)
                rd_free((void *)cpair->dst);
        rd_free(cpair);
}

static int ConsumerPair_cmp (const void *_a, const void *_b) {
        const ConsumerPair_t *a = _a, *b = _b;
        int r = strcmp(a->src ? a->src : "", b->src ? b->src : "");
        if (r)
                return r;
        return strcmp(a->dst ? a->dst : "", b->dst ? b->dst : "");
}


static unsigned int ConsumerPair_hash (const void *_a) {
        const ConsumerPair_t *a = _a;
        return 31 * (a->src ? rd_map_str_hash(a->src) : 1) +
                (a->dst ? rd_map_str_hash(a->dst) : 1);
}



typedef struct ConsumerGenerationPair_s {
        const char *consumer; /**< Memory owned by caller */
        int generation;
} ConsumerGenerationPair_t;

static void ConsumerGenerationPair_destroy (void *ptr) {
        ConsumerGenerationPair_t *cgpair = ptr;
        rd_free(cgpair);
}

/**
 * @param consumer This memory will be referenced, not copied, and thus must
 *                 outlive the ConsumerGenerationPair_t object.
 */
static ConsumerGenerationPair_t *
ConsumerGenerationPair_new (const char *consumer, int generation) {
        ConsumerGenerationPair_t *cgpair = rd_malloc(sizeof(*cgpair));
        cgpair->consumer = consumer;
        cgpair->generation = generation;
        return cgpair;
}

static int ConsumerGenerationPair_cmp_generation (const void *_a,
                                                  const void *_b) {
        const ConsumerGenerationPair_t *a = _a, *b = _b;
        return a->generation - b->generation;
}




/**
 * Hash map types.
 *
 * Naming convention is:
 * map_<keytype>_<valuetype>_t
 *
 * Where the keytype and valuetype are spoken names of the types and
 * not the specific C types (since that'd be too long).
 */
typedef RD_MAP_TYPE(const char *,
                    rd_kafka_topic_partition_list_t *) map_str_toppar_list_t;

typedef RD_MAP_TYPE(const rd_kafka_topic_partition_t *,
                    const char *) map_toppar_str_t;

typedef RD_MAP_TYPE(const rd_kafka_topic_partition_t *,
                    rd_list_t *) map_toppar_list_t;

typedef RD_MAP_TYPE(const rd_kafka_topic_partition_t *,
                    ConsumerGenerationPair_t *) map_toppar_cgpair_t;

typedef RD_MAP_TYPE(const rd_kafka_topic_partition_t *,
                    ConsumerPair_t *) map_toppar_cpair_t;

typedef RD_MAP_TYPE(const ConsumerPair_t *,
                    rd_kafka_topic_partition_list_t *) map_cpair_toppar_list_t;

/* map<string, map<ConsumerPair*, topic_partition_list_t*>> */
typedef RD_MAP_TYPE(const char *,
                    map_cpair_toppar_list_t *) map_str_map_cpair_toppar_list_t;



/** Glue type helpers */

static map_cpair_toppar_list_t *map_cpair_toppar_list_t_new (void) {
        map_cpair_toppar_list_t *map = rd_calloc(1, sizeof(*map));

        RD_MAP_INIT(map, 0, ConsumerPair_cmp, ConsumerPair_hash,
                    NULL, rd_kafka_topic_partition_list_destroy_free);

        return map;
}

static void map_cpair_toppar_list_t_free (void *ptr) {
        map_cpair_toppar_list_t *map = ptr;
        RD_MAP_DESTROY(map);
        rd_free(map);
}




/**
 * @struct Provides current state of partition movements between consumers
 *         for each topic, and possible movements for each partition.
 */
typedef struct PartitionMovements_s {
        map_toppar_cpair_t partitionMovements;
        map_str_map_cpair_toppar_list_t partitionMovementsByTopic;
} PartitionMovements_t;


static void PartitionMovements_init (PartitionMovements_t *pmov,
                                     size_t topic_cnt) {
        RD_MAP_INIT(&pmov->partitionMovements,
                    topic_cnt * 3,
                    rd_kafka_topic_partition_cmp,
                    rd_kafka_topic_partition_hash,
                    NULL,
                    ConsumerPair_free);

        RD_MAP_INIT(&pmov->partitionMovementsByTopic,
                    topic_cnt,
                    rd_map_str_cmp,
                    rd_map_str_hash,
                    NULL,
                    map_cpair_toppar_list_t_free);
}

static void PartitionMovements_destroy (PartitionMovements_t *pmov) {
        RD_MAP_DESTROY(&pmov->partitionMovementsByTopic);
        RD_MAP_DESTROY(&pmov->partitionMovements);
}


static ConsumerPair_t *PartitionMovements_removeMovementRecordOfPartition (
        PartitionMovements_t *pmov,
        const rd_kafka_topic_partition_t *toppar) {

        ConsumerPair_t *cpair;
        map_cpair_toppar_list_t *partitionMovementsForThisTopic;
        rd_kafka_topic_partition_list_t *plist;

        cpair = RD_MAP_GET(&pmov->partitionMovements, toppar);
        rd_assert(cpair);

        partitionMovementsForThisTopic =
                RD_MAP_GET(&pmov->partitionMovementsByTopic, toppar->topic);

        plist = RD_MAP_GET(partitionMovementsForThisTopic, cpair);
        rd_assert(plist);

        rd_kafka_topic_partition_list_del(plist,
                                          toppar->topic, toppar->partition);
        if (plist->cnt == 0)
                RD_MAP_DELETE(partitionMovementsForThisTopic, cpair);
        if (RD_MAP_IS_EMPTY(partitionMovementsForThisTopic))
                RD_MAP_DELETE(&pmov->partitionMovementsByTopic, toppar->topic);

        return cpair;
}

static void PartitionMovements_addPartitionMovementRecord (
        PartitionMovements_t *pmov,
        const rd_kafka_topic_partition_t *toppar,
        ConsumerPair_t *cpair) {
        map_cpair_toppar_list_t *partitionMovementsForThisTopic;
        rd_kafka_topic_partition_list_t *plist;

        RD_MAP_SET(&pmov->partitionMovements, toppar, cpair);

        partitionMovementsForThisTopic =
                RD_MAP_GET_OR_SET(&pmov->partitionMovementsByTopic,
                                  toppar->topic,
                                  map_cpair_toppar_list_t_new());

        plist = RD_MAP_GET_OR_SET(partitionMovementsForThisTopic,
                                  cpair,
                                  rd_kafka_topic_partition_list_new(16));

        rd_kafka_topic_partition_list_add(plist,
                                          toppar->topic, toppar->partition);
}

static void PartitionMovements_movePartition (
        PartitionMovements_t *pmov,
        const rd_kafka_topic_partition_t *toppar,
        const char *old_consumer, const char *new_consumer) {

        if (RD_MAP_GET(&pmov->partitionMovements, toppar)) {
                /* This partition has previously moved */
                ConsumerPair_t *existing_cpair;

                existing_cpair =
                        PartitionMovements_removeMovementRecordOfPartition(
                                pmov, toppar);

                rd_assert(!rd_strcmp(existing_cpair->dst, old_consumer));

                if (rd_strcmp(existing_cpair->src, new_consumer)) {
                        /* Partition is not moving back to its
                         * previous consumer */
                        PartitionMovements_addPartitionMovementRecord(
                                pmov, toppar,
                                ConsumerPair_new(existing_cpair->src,
                                                 new_consumer));
                }
        } else {
                PartitionMovements_addPartitionMovementRecord(
                        pmov, toppar,
                        ConsumerPair_new(old_consumer, new_consumer));
        }
}

static const rd_kafka_topic_partition_t *
PartitionMovements_getTheActualPartitionToBeMoved (
        PartitionMovements_t *pmov,
        const rd_kafka_topic_partition_t *toppar,
        const char *old_consumer, const char *new_consumer) {

        ConsumerPair_t *cpair;
        ConsumerPair_t reverse_cpair = { .src = new_consumer,
                                         .dst = old_consumer };
        map_cpair_toppar_list_t *partitionMovementsForThisTopic;
        rd_kafka_topic_partition_list_t *plist;

        if (!RD_MAP_GET(&pmov->partitionMovementsByTopic, toppar->topic))
                return toppar;

        cpair = RD_MAP_GET(&pmov->partitionMovements, toppar);
        if (cpair) {
                /* This partition has previously moved */
                rd_assert(!rd_strcmp(old_consumer, cpair->dst));

                old_consumer = cpair->src;
        }

        partitionMovementsForThisTopic =
                RD_MAP_GET(&pmov->partitionMovementsByTopic, toppar->topic);

        plist = RD_MAP_GET(partitionMovementsForThisTopic, &reverse_cpair);
        if (!plist)
                return toppar;

        return &plist->elems[0]; /* FIXME: should we do a next() here? */
}

static rd_bool_t PartitionMovements_isSticky (PartitionMovements_t *pmov) {
        /* FIXME: this method is only used by the AbstractStickyAssignorTest
         *        in the Java client. */
        return rd_true;
}



/**
 * @brief Comparator to sort ascendingly by rd_map_elem_t object key as
 *        topic partition list count.
 *        Used to sort sortedCurrentSubscriptions list.
 */
static int sort_by_map_elem_val_toppar_list_cnt (const void *_a,
                                                 const void *_b) {
        const rd_map_elem_t *a = _a, *b = _b;
        const rd_kafka_topic_partition_list_t *al = a->value, *bl = b->value;
        return al->cnt - bl->cnt;
}


/**
 * @brief Assign partition to the most eligible consumer.
 *
 * The assignment should improve the overall balance of the partition
 * assignments to consumers.
 */
static void
assignPartition (const rd_kafka_topic_partition_t *partition,
                 rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
                 map_str_toppar_list_t *currentAssignment,
                 map_str_toppar_list_t *consumer2AllPotentialPartitions,
                 map_toppar_str_t *currentPartitionConsumer) {
        const rd_map_elem_t *elem;
        int i;

        RD_LIST_FOREACH(elem, sortedCurrentSubscriptions, i) {
                const char *consumer = (const char *)elem->key;
                const rd_kafka_topic_partition_list_t *partitions;

                partitions = RD_MAP_GET(consumer2AllPotentialPartitions,
                                        consumer);
                if (!rd_kafka_topic_partition_list_find(partitions,
                                                        partition->topic,
                                                        partition->partition))
                        continue;

                rd_kafka_topic_partition_list_add(
                        RD_MAP_GET(currentAssignment, consumer),
                        partition->topic, partition->partition);

                RD_MAP_SET(currentPartitionConsumer, partition, consumer);

                /* Re-sort sortedCurrentSubscriptions since this consumer's
                 * assignment count has increased.
                 * This is an O(N) operation since it is a single shuffle. */
                rd_list_sort(sortedCurrentSubscriptions,
                             sort_by_map_elem_val_toppar_list_cnt);
                return;
        }
}

/**
 * @returns true if the partition has two or more potential consumers.
 */
static RD_INLINE rd_bool_t
partitionCanParticipateInReassignment (
        const rd_kafka_topic_partition_t *partition,
        map_toppar_list_t *partition2AllPotentialConsumers) {
        rd_list_t *consumers;

        if (!(consumers = RD_MAP_GET(partition2AllPotentialConsumers,
                                     partition)))
                return rd_false;

        return rd_list_cnt(consumers) >= 2;
}


/**
 * @returns true if consumer .. FIXME
 */
static RD_INLINE rd_bool_t
consumerCanParticipateInReassignment (
        rd_kafka_t *rk,
        const char *consumer,
        map_str_toppar_list_t *currentAssignment,
        map_str_toppar_list_t *consumer2AllPotentialPartitions,
        map_toppar_list_t *partition2AllPotentialConsumers) {
        const rd_kafka_topic_partition_list_t *currentPartitions =
                RD_MAP_GET(currentAssignment, consumer);
        int currentAssignmentSize = currentPartitions->cnt;
        int maxAssignmentSize = RD_MAP_GET(consumer2AllPotentialPartitions,
                                           consumer)->cnt;
        int i;

        /* FIXME: And then what? Is this a local error? If so, assert. */
        if (currentAssignmentSize > maxAssignmentSize)
                rd_kafka_log(rk, LOG_ERR, "STICKY",
                             "Sticky assignor error: "
                             "Consumer %s is assigned more partitions (%d) "
                             "than the maximum possible (%d)",
                             consumer, currentAssignmentSize,
                             maxAssignmentSize);

        /* If a consumer is not assigned all its potential partitions it is
         * subject to reassignment. */
        if (currentAssignmentSize < maxAssignmentSize)
                return rd_true;

        /* If any of the partitions assigned to a consumer is subject to
         * reassignment the consumer itself is subject to reassignment. */
        for (i = 0 ; i < currentPartitions->cnt ; i++) {
                const rd_kafka_topic_partition_t *partition =
                        &currentPartitions->elems[i];

                if (partitionCanParticipateInReassignment(
                            partition, partition2AllPotentialConsumers))
                        return rd_true;
        }

        return rd_false;
}


/**
 * @brief Process moving partition from old consumer to new consumer.
 */
static void processPartitionMovement (
        PartitionMovements_t *partitionMovements,
        const rd_kafka_topic_partition_t *partition,
        const char *newConsumer,
        map_str_toppar_list_t *currentAssignment,
        rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
        map_toppar_str_t *currentPartitionConsumer) {

        const char *oldConsumer = RD_MAP_GET(currentPartitionConsumer,
                                             partition);

        PartitionMovements_movePartition(partitionMovements, partition,
                                         oldConsumer, newConsumer);

        rd_kafka_topic_partition_list_add(RD_MAP_GET(currentAssignment,
                                                     newConsumer),
                                          partition->topic,
                                          partition->partition);

        rd_kafka_topic_partition_list_del(RD_MAP_GET(currentAssignment,
                                                     oldConsumer),
                                          partition->topic,
                                          partition->partition);

        RD_MAP_SET(currentPartitionConsumer, partition, newConsumer);

        /* Re-sort after assignment count has changed. */
        rd_list_sort(sortedCurrentSubscriptions,
                     sort_by_map_elem_val_toppar_list_cnt);
}


/**
 * @brief Reassign \p partition to \p newConsumer
 */
static void
reassignPartitionToConsumer (
        PartitionMovements_t *partitionMovements,
        const rd_kafka_topic_partition_t *partition,
        map_str_toppar_list_t *currentAssignment,
        rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
        map_toppar_str_t *currentPartitionConsumer,
        const char *newConsumer) {

        const char *consumer = RD_MAP_GET(currentPartitionConsumer, partition);
        const rd_kafka_topic_partition_t *partitionToBeMoved;

        /* Find the correct partition movement considering
         * the stickiness requirement. */
        partitionToBeMoved =
                PartitionMovements_getTheActualPartitionToBeMoved(
                        partitionMovements,
                        partition,
                        consumer,
                        newConsumer);

        processPartitionMovement(
                partitionMovements,
                partitionToBeMoved,
                newConsumer,
                currentAssignment,
                sortedCurrentSubscriptions,
                currentPartitionConsumer);
}

/**
 * @brief Reassign \p partition to an eligible new consumer.
 */
static void reassignPartition (
        PartitionMovements_t *partitionMovements,
        const rd_kafka_topic_partition_t *partition,
        map_str_toppar_list_t *currentAssignment,
        rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
        map_toppar_str_t *currentPartitionConsumer,
        map_str_toppar_list_t *consumer2AllPotentialPartitions) {

        const rd_map_elem_t *elem;
        int i;

        /* Find the new consumer */
        RD_LIST_FOREACH(elem, sortedCurrentSubscriptions, i) {
                const char *newConsumer = (const char *)elem->key;

                if (rd_kafka_topic_partition_list_find(
                            RD_MAP_GET(consumer2AllPotentialPartitions,
                                       newConsumer),
                            partition->topic,
                            partition->partition)) {

                        reassignPartitionToConsumer(
                                partitionMovements,
                                partition,
                                currentAssignment,
                                sortedCurrentSubscriptions,
                                currentPartitionConsumer,
                                newConsumer);

                        return;
                }
        }

        rd_assert(!*"reassignPartition(): no new consumer found");
}



/**
 * @brief Determine if the current assignment is balanced.
 *
 * @param currentAssignment the assignment whose balance needs to be checked
 * @param sortedCurrentSubscriptions an ascending sorted set of consumers based
 *                                   on how many topic partitions are already
 *                                   assigned to them
 * @param consumer2AllPotentialPartitions a mapping of all consumers to all
 *                                        potential topic partitions that can be
 *                                        assigned to them.
 *                                        This parameter is called
 *                                        allSubscriptions in the Java
 *                                        implementation, but we choose this
 *                                        name be more consistent with its
 *                                        use elsewhere in the code.
 *
 * @returns true if the given assignment is balanced; false otherwise
 */
static rd_bool_t
isBalanced (map_str_toppar_list_t *currentAssignment,
            const rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
            map_str_toppar_list_t *consumer2AllPotentialPartitions) {

        return rd_true; /* FIXME */

#if 0
        int min = currentAssignment.get(sortedCurrentSubscriptions.first()).size();
        int max = currentAssignment.get(sortedCurrentSubscriptions.last()).size();
        if (min >= max - 1)
                // if minimum and maximum numbers of partitions assigned to consumers differ by at most one return true
                return true;

        // create a mapping from partitions to the consumer assigned to them
        final Map<TopicPartition, String> allPartitions = new HashMap<>();
        Set<Entry<String, List<TopicPartition>>> assignments = currentAssignment.entrySet();
        for (Map.Entry<String, List<TopicPartition>> entry: assignments) {
                List<TopicPartition> topicPartitions = entry.getValue();
                for (TopicPartition topicPartition: topicPartitions) {
                        if (allPartitions.containsKey(topicPartition))
                                log.error("{} is assigned to more than one consumer.", topicPartition);
                        allPartitions.put(topicPartition, entry.getKey());
                }
        }

        // for each consumer that does not have all the topic partitions it can get make sure none of the topic partitions it
        // could but did not get cannot be moved to it (because that would break the balance)
        for (String consumer: sortedCurrentSubscriptions) {
                List<TopicPartition> consumerPartitions = currentAssignment.get(consumer);
                int consumerPartitionCount = consumerPartitions.size();

                // skip if this consumer already has all the topic partitions it can get
                if (consumerPartitionCount == allSubscriptions.get(consumer).size())
                        continue;

                // otherwise make sure it cannot get any more
                List<TopicPartition> potentialTopicPartitions = allSubscriptions.get(consumer);
                for (TopicPartition topicPartition: potentialTopicPartitions) {
                        if (!currentAssignment.get(consumer).contains(topicPartition)) {
                                String otherConsumer = allPartitions.get(topicPartition);
                                int otherConsumerPartitionCount = currentAssignment.get(otherConsumer).size();
                                if (consumerPartitionCount < otherConsumerPartitionCount) {
                                        log.debug("{} can be moved from consumer {} to consumer {} for a more balanced assignment.",
                                                  topicPartition, otherConsumer, consumer);
                                        return false;
                                }
                        }
                }
        }
        return true;
#endif
}


/**
 * @brief Perform reassignment.
 *
 * @returns true if reassignment was performed.
 */
static rd_bool_t
performReassignments (
        rd_kafka_t *rk,
        PartitionMovements_t *partitionMovements,
        rd_kafka_topic_partition_list_t *reassignablePartitions,
        map_str_toppar_list_t *currentAssignment,
        map_toppar_cgpair_t *prevAssignment,
        rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
        map_str_toppar_list_t *consumer2AllPotentialPartitions,
        map_toppar_list_t *partition2AllPotentialConsumers,
        map_toppar_str_t *currentPartitionConsumer) {
        rd_bool_t reassignmentPerformed = rd_false;
        rd_bool_t modified;

        /* Repeat reassignment until no partition can be moved to
         * improve the balance. */
        do {
                int i;

                modified = rd_false;

                /* Reassign all reassignable partitions (starting from the
                 * partition with least potential consumers and if needed)
                 * until the full list is processed or a balance is achieved. */

                for (i = 0 ; i < reassignablePartitions->cnt &&
                             !isBalanced(currentAssignment,
                                         sortedCurrentSubscriptions,
                                         consumer2AllPotentialPartitions) ;
                     i++) {
                        const rd_kafka_topic_partition_t *partition =
                                &reassignablePartitions->elems[i];
                        const rd_list_t *consumers =
                                RD_MAP_GET(partition2AllPotentialConsumers,
                                           partition);
                        const char *consumer, *otherConsumer;
                        const ConsumerGenerationPair_t *prevcgp;
                        const rd_kafka_topic_partition_list_t *currAssignment;
                        int j;

                        /* FIXME: Is this a local error/bug? If so, assert */
                        if (rd_list_cnt(consumers) <= 1)
                                rd_kafka_log(
                                        rk, LOG_ERR, "STICKY",
                                        "Sticky assignor: expected more than "
                                        "one potential consumer for partition "
                                        "%s [%"PRId32"]",
                                        partition->topic,
                                        partition->partition);

                        /* The partition must have a current consumer */
                        consumer = RD_MAP_GET(currentPartitionConsumer,
                                              partition);
                        rd_assert(consumer);

                        currAssignment = RD_MAP_GET(currentAssignment,
                                                    consumer);
                        prevcgp = RD_MAP_GET(prevAssignment, partition);

                        if (prevcgp &&
                            currAssignment->cnt >
                            RD_MAP_GET(currentAssignment,
                                       prevcgp->consumer)->cnt + 1) {
                                reassignPartitionToConsumer(
                                        partitionMovements,
                                        partition,
                                        currentAssignment,
                                        sortedCurrentSubscriptions,
                                        currentPartitionConsumer,
                                        prevcgp->consumer);
                                reassignmentPerformed = rd_true;
                                modified = rd_true;
                                continue;
                        }

                        /* Check if a better-suited consumer exists for the
                         * partition; if so, reassign it. */

                        RD_LIST_FOREACH(otherConsumer, consumers, j) {
                                if (currAssignment->cnt <=
                                    RD_MAP_GET(currentAssignment,
                                               otherConsumer)->cnt + 1)
                                        continue;

                                reassignPartition(
                                        partitionMovements,
                                        partition,
                                        currentAssignment,
                                        sortedCurrentSubscriptions,
                                        currentPartitionConsumer,
                                        consumer2AllPotentialPartitions);

                                reassignmentPerformed = rd_true;
                                modified = rd_true;
                                break;
                        }
                }
        } while (modified);

        return reassignmentPerformed;
}


/**
 * @returns the balance score of the given assignment, as the sum of assigned
 *           partitions size difference of all consumer pairs.
 *
 * A perfectly balanced assignment (with all consumers getting the same number
 * of partitions) has a balance score of 0.
 *
 * Lower balance score indicates a more balanced assignment.
 * FIXME: should be called imbalance score then?
 */
static int getBalanceScore (map_str_toppar_list_t *assignment) {
        const char *consumer;
        const rd_kafka_topic_partition_list_t *partitions;
        int *sizes;
        int cnt = RD_MAP_CNT(assignment);
        int score = 0;
        int i, next;

        /* If there is just a single consumer the assignment will be balanced */
        if (cnt < 2)
                return 0;

        sizes = rd_malloc(sizeof(*sizes) * cnt);

        RD_MAP_FOREACH(consumer, partitions, assignment) {
                sizes[i] = partitions->cnt;
        }

        for (next = 0 ; next < cnt ; next++)
                for (i = next+1 ; i < cnt ; i++)
                        score = abs(sizes[next] - sizes[i]);

        rd_free(sizes);

        if (consumer)
                ; /* Avoid unused warning */

        return score;
}



/**
 * @brief Balance the current assignment using the data structures
 *        created in assign_cb(). */
static void
balance (rd_kafka_t *rk,
         PartitionMovements_t *partitionMovements,
         map_str_toppar_list_t *currentAssignment,
         map_toppar_cgpair_t *prevAssignment,
         rd_kafka_topic_partition_list_t *sortedPartitions,
         rd_kafka_topic_partition_list_t *unassignedPartitions,
         rd_list_t *sortedCurrentSubscriptions /*rd_map_elem_t*/,
         map_str_toppar_list_t *consumer2AllPotentialPartitions,
         map_toppar_list_t *partition2AllPotentialConsumers,
         map_toppar_str_t *currentPartitionConsumer,
         rd_bool_t revocationRequired) {

        /* If the consumer with most assignments (thus the last element
         * in the ascendingly ordered sortedCurrentSubscriptions list) has
         * zero partitions assigned it means there is no current assignment
         * for any consumer and the group is thus initializing for the first
         * time. */
        rd_bool_t initializing =
                ((const rd_kafka_topic_partition_list_t *)
                 ((const rd_map_elem_t *)rd_list_last(
                         sortedCurrentSubscriptions))->key)->cnt == 0;
        rd_bool_t reassignmentPerformed = rd_false;

        map_str_toppar_list_t fixedAssignments =
                RD_MAP_INITIALIZER(RD_MAP_CNT(partition2AllPotentialConsumers),
                                   rd_map_str_cmp,
                                   rd_map_str_hash,
                                   NULL,
                                   NULL /* Will transfer ownership of the list
                                         * to currentAssignment at the end of
                                         * this function. */);

        map_str_toppar_list_t preBalanceAssignment;
        map_toppar_str_t preBalancePartitionConsumers;

        /* Iterator variables */
        const rd_kafka_topic_partition_t *partition;
        const void *ignore;
        const rd_map_elem_t *elem;
        int i;

        /* Assign all unassigned partitions */
        for (i = 0 ; i < unassignedPartitions->cnt ; i++) {
                partition = &unassignedPartitions->elems[i];

                /* Skip if there is no potential consumer for the partition.
                 * FIXME: How could this be? */
                if (rd_list_empty(RD_MAP_GET(partition2AllPotentialConsumers,
                                             partition)))
                        continue;

                assignPartition(partition, sortedCurrentSubscriptions,
                                currentAssignment,
                                consumer2AllPotentialPartitions,
                                currentPartitionConsumer);
        }


        /* Narrow down the reassignment scope to only those partitions that can
         * actually be reassigned. */
        RD_MAP_FOREACH(partition, ignore, partition2AllPotentialConsumers) {
                if (partitionCanParticipateInReassignment(
                            partition, partition2AllPotentialConsumers))
                        continue;

                rd_kafka_topic_partition_list_del(sortedPartitions,
                                                  partition->topic,
                                                  partition->partition);
                rd_kafka_topic_partition_list_del(unassignedPartitions,
                                                  partition->topic,
                                                  partition->partition);
        }

        if (ignore)
                ; /* Avoid unused warning */


        /* Narrow down the reassignment scope to only those consumers that are
         * subject to reassignment. */
        RD_LIST_FOREACH(elem, sortedCurrentSubscriptions, i) {
                const char *consumer = (const char *)elem->key;
                rd_kafka_topic_partition_list_t *partitions;

                if (consumerCanParticipateInReassignment(
                            rk,
                            consumer,
                            currentAssignment,
                            consumer2AllPotentialPartitions,
                            partition2AllPotentialConsumers))
                        continue;

                rd_list_remove_elem(sortedCurrentSubscriptions, i);
                i--; /* Since the current element is removed we need
                      * to rewind the iterator. */

                partitions = rd_kafka_topic_partition_list_copy(
                        RD_MAP_GET(currentAssignment, consumer));
                RD_MAP_DELETE(currentAssignment, consumer);

                RD_MAP_SET(&fixedAssignments, consumer, partitions);
        }


        /* Create a deep copy of the current assignment so we can revert to it
         * if we do not get a more balanced assignment later. */
        RD_MAP_COPY(&preBalanceAssignment, currentAssignment,
                    NULL /* just reference the key */,
                    (rd_map_copy_t *)rd_kafka_topic_partition_list_copy);
        RD_MAP_COPY(&preBalancePartitionConsumers, currentPartitionConsumer,
                    NULL /* references assign_cb(members) fields */,
                    NULL /* references assign_cb(members) fields */);


        /* If we don't already need to revoke something due to subscription
         * changes, first try to balance by only moving newly added partitions.
         */
        if (!revocationRequired)
                performReassignments(rk,
                                     partitionMovements,
                                     unassignedPartitions,
                                     currentAssignment,
                                     prevAssignment,
                                     sortedCurrentSubscriptions,
                                     consumer2AllPotentialPartitions,
                                     partition2AllPotentialConsumers,
                                     currentPartitionConsumer);

        reassignmentPerformed =
                performReassignments(rk,
                                     partitionMovements,
                                     sortedPartitions,
                                     currentAssignment,
                                     prevAssignment,
                                     sortedCurrentSubscriptions,
                                     consumer2AllPotentialPartitions,
                                     partition2AllPotentialConsumers,
                                     currentPartitionConsumer);

        /* If we are not preserving existing assignments and we have made
         * changes to the current assignment make sure we are getting a more
         * balanced assignment; otherwise, revert to previous assignment. */

        if (!initializing && reassignmentPerformed &&
            getBalanceScore(currentAssignment) >=
            getBalanceScore(&preBalanceAssignment)) {

                RD_MAP_COPY(currentAssignment, &preBalanceAssignment,
                            NULL /* just reference the key */,
                            (rd_map_copy_t*)rd_kafka_topic_partition_list_copy);

                RD_MAP_CLEAR(currentPartitionConsumer);
                RD_MAP_COPY(currentPartitionConsumer,
                            &preBalancePartitionConsumers,
                            NULL /* references assign_cb(members) fields */,
                            NULL /* references assign_cb(members) fields */);
        }

        RD_MAP_DESTROY(&preBalancePartitionConsumers);
        RD_MAP_DESTROY(&preBalanceAssignment);

        /* Add the fixed assignments (those that could not change) back. */
        if (!RD_MAP_IS_EMPTY(&fixedAssignments)) {
                const char *consumer;
                rd_kafka_topic_partition_list_t *partitions;

                RD_MAP_FOREACH(consumer, partitions, &fixedAssignments) {
                        RD_MAP_SET(currentAssignment, consumer,
                                   /* Transfer ownership to currentAssignment */
                                   partitions);

                        rd_list_add(sortedCurrentSubscriptions,
                                    (void *)consumer);
                }

                /* Re-sort */
                rd_list_sort(sortedCurrentSubscriptions,
                             sort_by_map_elem_val_toppar_list_cnt);
        }

        RD_MAP_DESTROY(&fixedAssignments);
}










/**
 * @brief Populate subscriptions, current and previous assignments based on the
 *        \p members assignments.
 */
static void
preopulateCurrentAssignments (rd_kafka_t *rk,
                              rd_kafka_group_member_t *members,
                              size_t member_cnt,
                              map_str_toppar_list_t *subscriptions,
                              map_str_toppar_list_t *currentAssignment,
                              map_toppar_cgpair_t *prevAssignment,
                              map_toppar_str_t *currentPartitionConsumer) {
        /* We need to process subscriptions' user data with each consumer's
         * reported generation in mind.
         * Higher generations overwrite lower generations in case of a conflict.
         * Conflicts will only exist if user data is for different generations.
         */

        /* For each partition we create a sorted list (by generation) of
         * its consumers. */
        RD_MAP_LOCAL_INITIALIZER(sortedPartitionConsumersByGeneration,
                                 member_cnt * 10 /* FIXME */,
                                 const rd_kafka_topic_partition_t *,
                                 /* List of ConsumerGenerationPair_t */
                                 rd_list_t *,
                                 rd_kafka_topic_partition_cmp,
                                 rd_kafka_topic_partition_hash,
                                 NULL,
                                 rd_list_destroy_free);
        const rd_kafka_topic_partition_t *partition;
        rd_list_t *consumers;
        int i;

        /* For each partition that is currently assigned to the group members
         * add the member and its generation to
         * sortedPartitionConsumersByGeneration (which is sorted afterwards)
         * indexed by the partition. */
        for (i = 0 ; i < (int)member_cnt ; i++) {
                rd_kafka_group_member_t *consumer = &members[i];
                int j;

                RD_MAP_SET(subscriptions, consumer->rkgm_member_id->str,
                           consumer->rkgm_subscription);

                RD_MAP_SET(currentAssignment, consumer->rkgm_member_id->str,
                           rd_kafka_topic_partition_list_new(10));

                if (!consumer->rkgm_assignment)
                        continue;

                for (j = 0 ; j < (int)consumer->rkgm_assignment->cnt ; j++) {
                        partition = &consumer->rkgm_assignment->elems[j];

                        consumers = RD_MAP_GET_OR_SET(
                                &sortedPartitionConsumersByGeneration,
                                partition,
                                rd_list_new(10,
                                            ConsumerGenerationPair_destroy));

                        if (consumer->rkgm_generation != -1 &&
                            rd_list_find(
                                    consumers, &consumer->rkgm_generation,
                                    ConsumerGenerationPair_cmp_generation)) {
                                rd_kafka_log(rk, LOG_WARNING, "STICKY",
                                             "Sticky assignor: "
                                             "%s [%"PRId32"] is assigned to "
                                             "multiple consumers with same "
                                             "generation %d: "
                                             "skipping member %.*s",
                                             partition->topic,
                                             partition->partition,
                                             consumer->rkgm_generation,
                                             RD_KAFKAP_STR_PR(consumer->
                                                              rkgm_member_id));
                                continue;
                        }

                        rd_list_add(consumers,
                                    ConsumerGenerationPair_new(
                                            consumer->rkgm_member_id->str,
                                            consumer->rkgm_generation));

                        RD_MAP_SET(currentPartitionConsumer,
                                   partition, consumer->rkgm_member_id->str);
                }
        }

        /* Populate currentAssignment and prevAssignment.
         * prevAssignment holds the prior ConsumerGenerationPair_t
         * (before current) of each partition. */
        RD_MAP_FOREACH(partition, consumers,
                       &sortedPartitionConsumersByGeneration) {
                /* current and previous are the last two consumers
                 * of each partition. */
                ConsumerGenerationPair_t *current, *previous;
                rd_kafka_topic_partition_list_t *partitions;

                /* Sort the per-partition consumers list by generation */
                rd_list_sort(consumers, ConsumerGenerationPair_cmp_generation);

                /* Add current (highest generation) consumer
                 * to currentAssignment. */
                current = rd_list_elem(consumers, 0);
                partitions = RD_MAP_GET(currentAssignment, current->consumer);
                rd_kafka_topic_partition_list_add(partitions,
                                                  partition->topic,
                                                  partition->partition);

                /* Add previous (next highest generation) consumer, if any,
                 * to prevAssignment. */
                previous = rd_list_elem(consumers, 1);
                if (previous)
                        RD_MAP_SET(prevAssignment, partition,
                                   ConsumerGenerationPair_new(
                                           previous->consumer,
                                           previous->generation));
        }

        RD_MAP_DESTROY(&sortedPartitionConsumersByGeneration);
}


/**
 * @brief Populate maps for potential partitions per consumer and vice-versa.
 */
static void populatePotentialMaps (
        const rd_kafka_assignor_topic_t *atopic,
        map_toppar_list_t *partition2AllPotentialConsumers,
        map_str_toppar_list_t *consumer2AllPotentialPartitions,
        size_t estimated_partition_cnt) {
        int i;
        const char *consumer;

        /* for each eligible (subscribed and available) topic (\p atopic):
         *   for each member subscribing to that topic:
         *     and for each partition of that topic:
         *        add conusmer and partition to:
         *          partition2AllPotentialConsumers
         *          consumer2AllPotentialPartitions
         */

        RD_LIST_FOREACH(consumer, &atopic->members, i) {
                int j;

                for (j = 0 ; j < atopic->metadata->partition_cnt ; j++) {
                        rd_list_t *consumers =
                                RD_MAP_GET_OR_SET(
                                        partition2AllPotentialConsumers,
                                        rd_kafka_topic_partition_new(
                                                atopic->metadata->topic,
                                                atopic->metadata->
                                                partitions[j].id),
                                        rd_list_new(
                                                RD_MAX(2,
                                                       estimated_partition_cnt/
                                                       2), NULL));
                        rd_kafka_topic_partition_list_t *partitions =
                                RD_MAP_GET_OR_SET(
                                        consumer2AllPotentialPartitions,
                                        consumer,
                                        rd_kafka_topic_partition_list_new(
                                                estimated_partition_cnt));

                        /* partition2AllPotentialConsumers[part] += consumer */
                        rd_list_add(consumers, (void *)consumer);

                        /* consumer2AllPotentialPartitions[consumer] += part */
                        rd_kafka_topic_partition_list_add(
                                partitions,
                                atopic->metadata->topic,
                                atopic->metadata->partitions[j].id);
                }
        }
}


/**
 * @returns true if all consumers have identical subscriptions based on
 *          the currently available topics and partitions.
 *
 * @remark The Java code checks both partition2AllPotentialConsumers and
 *         and consumer2AllPotentialPartitions but since these maps
 *         are symmetrical we only check one of them.
 */
static rd_bool_t areSubscriptionsIdentical (
        map_toppar_list_t *partition2AllPotentialConsumers,
        map_str_toppar_list_t *consumer2AllPotentialPartitions) {
        const void *ignore;
        const rd_list_t *lcurr, *lprev = NULL;
        const rd_kafka_topic_partition_list_t *pcurr, *pprev;

        RD_MAP_FOREACH(ignore, lcurr, partition2AllPotentialConsumers) {
                if (lprev && rd_list_cmp(lcurr, lprev, rd_map_str_cmp))
                        return rd_false;
                lprev = lcurr;
        }

        RD_MAP_FOREACH(ignore, pcurr, consumer2AllPotentialPartitions) {
                if (pprev && rd_kafka_topic_partition_list_cmp(
                            pcurr, pprev, rd_kafka_topic_partition_cmp))
                        return rd_false;
                pprev = pcurr;
        }

        if (ignore) /* Avoid unused warning */
                ;

        return rd_true;
}


/**
 * @brief Comparator to sort an rd_kafka_topic_partition_list_t in ascending
 *        order by the number of list elements in the .opaque field.
 *        Used by sortPartitions().
 */
static int toppar_sort_by_list_cnt (const void *_a, const void *_b,
                                    void *opaque) {
        const rd_kafka_topic_partition_t *a = _a, *b = _b;
        const rd_list_t *al = a->opaque, *bl = b->opaque;
        return rd_list_cnt(al) - rd_list_cnt(bl); /* ascending order */
}


/**
 * @brief Sort valid partitions so they are processed in the potential
 *        reassignment phase in the proper order that causes minimal partition
 *        movement among consumers (hence honouring maximal stickiness).
 *
 * @returns The result of the partitions sort.
 */
static rd_kafka_topic_partition_list_t *
sortPartitions (map_str_toppar_list_t *currentAssignment,
                map_toppar_cgpair_t *prevAssignment,
                rd_bool_t isFreshAssignment,
                map_toppar_list_t *partition2AllPotentialConsumers,
                map_str_toppar_list_t *consumer2AllPotentialPartitions) {

        rd_kafka_topic_partition_list_t *sortedPartitions;
        map_str_toppar_list_t assignments =
                RD_MAP_INITIALIZER(RD_MAP_CNT(currentAssignment),
                                   rd_map_str_cmp,
                                   rd_map_str_hash,
                                   NULL,
                                   rd_kafka_topic_partition_list_destroy_free);
        const rd_kafka_topic_partition_list_t *partitions;
        const rd_kafka_topic_partition_t *partition;
        const rd_list_t *consumers;
        const char *consumer;
        rd_list_t sortedConsumers;  /* element is the (rd_map_elem_t *) from
                                     * assignments. */
        const rd_map_elem_t *elem;
        int i;

        sortedPartitions = rd_kafka_topic_partition_list_new(
                RD_MAP_CNT(partition2AllPotentialConsumers));;

        if (isFreshAssignment ||
            !areSubscriptionsIdentical(partition2AllPotentialConsumers,
                                       consumer2AllPotentialPartitions)) {
                /* Create an ascending sorted list of partitions based on
                 * how many consumers can potentially use them. */
                RD_MAP_FOREACH(partition, consumers,
                               partition2AllPotentialConsumers) {
                        rd_kafka_topic_partition_list_add(
                                sortedPartitions,
                                partition->topic,
                                partition->partition)->opaque =
                                (void *)consumers;
                }

                rd_kafka_topic_partition_list_sort(sortedPartitions,
                                                   toppar_sort_by_list_cnt,
                                                   NULL);

                return sortedPartitions;
        }

        /* If this is a reassignment and the subscriptions are identical
         * then we just need to list partitions in a round robin fashion
         * (from consumers with most assigned partitions to those
         * with least assigned partitions). */

        /* Create an ascorted sorted list of consumers by valid
         * partition count. The list element is the `rd_map_elem_t *`
         * of the assignments map. This allows us to get a sorted list
         * of consumers without too much data duplication. */
        rd_list_init(&sortedConsumers, RD_MAP_CNT(currentAssignment), NULL);

        RD_MAP_FOREACH(consumer, partitions, currentAssignment) {
                rd_kafka_topic_partition_list_t *partitions2;

                partitions2 =
                        rd_kafka_topic_partition_list_new(partitions->cnt);

                for (i = 0 ; i < partitions->cnt ; i++) {
                        partition = &partitions->elems[i];

                        /* Only add partitions from the current assignment
                         * that still exist. */
                        if (!RD_MAP_GET(partition2AllPotentialConsumers,
                                        partition))
                                rd_kafka_topic_partition_list_add(
                                        partitions2,
                                        partition->topic,
                                        partition->partition);
                }

                if (partitions2->cnt > 0) {
                        elem = RD_MAP_SET(&assignments, consumer, partitions2);
                        rd_list_add(&sortedConsumers, (void *)elem);
                } else
                        rd_kafka_topic_partition_list_destroy(partitions2);
        }

        /* Sort consumers */
        rd_list_sort(&sortedConsumers, sort_by_map_elem_val_toppar_list_cnt);

        /* At this point sortedConsumers contains an ascending-sorted list
         * of consumers based on how many valid partitions are currently
         * assigned to them. */

        while (!rd_list_empty(&sortedConsumers)) {
                /* Take consumer with most partitions */
                const rd_map_elem_t *elem = rd_list_last(&sortedConsumers);
                const char *consumer = (const char *)elem->key;
                /* Currently assigned partitions to this consumer */
                rd_kafka_topic_partition_list_t *remainingPartitions =
                        RD_MAP_GET(&assignments, consumer);
                /* Partitions that were assigned to a different consumer
                 * last time */
                rd_kafka_topic_partition_list_t *prevPartitions =
                        rd_kafka_topic_partition_list_new(
                                RD_MAP_CNT(prevAssignment));

                /* From the partitions that had a different consumer before,
                 * keep only those that are assigned to this consumer now. */
                for (i = 0 ; i < remainingPartitions->cnt ; i++) {
                        partition = &remainingPartitions->elems[i];
                        if (RD_MAP_GET(prevAssignment, partition))
                                rd_kafka_topic_partition_list_add(
                                        prevPartitions,
                                        partition->topic,
                                        partition->partition);
                }

                if (prevPartitions->cnt > 0) {
                        /* If there is a partition of this consumer that was
                         * assigned to another consumer before, then mark
                         * it as a good option for reassignment. */
                        partition =
                                &prevPartitions->elems[prevPartitions->cnt-1];

                        rd_kafka_topic_partition_list_del(
                                remainingPartitions,
                                partition->topic,
                                partition->partition);

                        rd_kafka_topic_partition_list_add(
                                sortedPartitions,
                                partition->topic,
                                partition->partition);

                        rd_kafka_topic_partition_list_del_by_idx(
                                prevPartitions,
                                prevPartitions->cnt-1);

                } else if (remainingPartitions->cnt > 0) {
                        /* Otherwise mark any other one of the current
                         * partitions as a reassignment candidate. */
                        partition = &remainingPartitions->elems[0];
                        rd_kafka_topic_partition_list_add(
                                sortedPartitions,
                                partition->topic,
                                partition->partition);

                        rd_kafka_topic_partition_list_del(
                                remainingPartitions,
                                partition->topic,
                                partition->partition);
                }

                /* Re-sort the list to keep the consumer with the most
                 * partitions at the end of the list.
                 * This should be an O(N) operation given it is at most a
                 * single shuffle. */
                rd_list_sort(&sortedConsumers,
                             sort_by_map_elem_val_toppar_list_cnt);
        }


        RD_MAP_FOREACH(partition, consumers, partition2AllPotentialConsumers)
                rd_kafka_topic_partition_list_upsert(sortedPartitions,
                                                     partition->topic,
                                                     partition->partition);

        rd_list_destroy(&sortedConsumers);
        RD_MAP_DESTROY(&assignments);

        return sortedPartitions;
}

/**
 * @brief KIP-54 and KIP-341/FIXME sticky assignor.
 *
 * This code is closely mimicking the AK Java AbstractStickyAssignor.assign().
 */
rd_kafka_resp_err_t
rd_kafka_sticky_assignor_assign_cb (rd_kafka_t *rk,
                                    const char *member_id,
                                    const char *protocol_name,
                                    const rd_kafka_metadata_t *metadata,
                                    rd_kafka_group_member_t *members,
                                    size_t member_cnt,
                                    rd_kafka_assignor_topic_t **eligible_topics,
                                    size_t eligible_topic_cnt,
                                    char *errstr, size_t errstr_size,
                                    void *opaque) {
        /* FIXME: Let the cgrp pass the actual eligible partition count */
        size_t partition_cnt = member_cnt * 10; /* FIXME */

        /* Map of subscriptions. This is \p member turned into a map. */
        map_str_toppar_list_t subscriptions =
                RD_MAP_INITIALIZER(member_cnt,
                                   rd_map_str_cmp,
                                   rd_map_str_hash,
                                   NULL /* refs members.rkgm_member_id */,
                                   NULL /* refs members.rkgm_subscription */);

        /* Map member to current assignment */
        map_str_toppar_list_t currentAssignment =
                RD_MAP_INITIALIZER(member_cnt,
                                   rd_map_str_cmp, rd_map_str_hash,
                                   NULL /* refs members.rkgm_member_id */,
                                   rd_kafka_topic_partition_list_destroy_free);

        /* Map partition to ConsumerGenerationPair */
        map_toppar_cgpair_t prevAssignment =
                RD_MAP_INITIALIZER(partition_cnt,
                                   rd_kafka_topic_partition_cmp,
                                   rd_kafka_topic_partition_hash,
                                   NULL,
                                   ConsumerGenerationPair_destroy);

        /* Partition assignment movements between consumers */
        PartitionMovements_t partitionMovements;

        rd_bool_t isFreshAssignment;

        /* Mapping of all topic partitions to all consumers that can be
         * assigned to them.
         * Value is an rd_list_t* with elements referencing the \p members
         * \c rkgm_member_id->str. */
        map_toppar_list_t partition2AllPotentialConsumers =
                RD_MAP_INITIALIZER(partition_cnt,
                                   rd_kafka_topic_partition_cmp,
                                   rd_kafka_topic_partition_hash,
                                   rd_kafka_topic_partition_destroy_free,
                                   rd_list_destroy_free);

        /* Mapping of all consumers to all potential topic partitions that
         * can be assigned to them. */
        map_str_toppar_list_t consumer2AllPotentialPartitions =
                RD_MAP_INITIALIZER(member_cnt,
                                   rd_map_str_cmp,
                                   rd_map_str_hash,
                                   NULL,
                                   rd_kafka_topic_partition_list_destroy_free);

        /* Mapping of partition to current consumer.
         * References \p members fields. */
        map_toppar_str_t currentPartitionConsumer =
                RD_MAP_INITIALIZER(partition_cnt,
                                   rd_kafka_topic_partition_cmp,
                                   rd_kafka_topic_partition_hash,
                                   NULL,
                                   NULL);

        rd_kafka_topic_partition_list_t *sortedPartitions;
        rd_kafka_topic_partition_list_t *unassignedPartitions;
        rd_list_t sortedCurrentSubscriptions;

        rd_bool_t revocationRequired = rd_false;

        /* Iteration variables */
        const char *consumer;
        rd_kafka_topic_partition_list_t *partitions;
        rd_map_elem_t *elem;
        int i;

        /* Initialize PartitionMovements */
        PartitionMovements_init(&partitionMovements, eligible_topic_cnt);

        /* Prepopulate current and previous assignments */
        preopulateCurrentAssignments(rk,
                                     members, member_cnt,
                                     &subscriptions,
                                     &currentAssignment,
                                     &prevAssignment,
                                     &currentPartitionConsumer);

        isFreshAssignment = RD_MAP_IS_EMPTY(&currentAssignment);

        /* Populate partition2AllPotentialConsumers and
         * consumer2AllPotentialPartitions maps by each eligible topic. */
        for (i = 0 ; i < (int)eligible_topic_cnt ; i++)
                populatePotentialMaps(eligible_topics[i],
                                      &partition2AllPotentialConsumers,
                                      &consumer2AllPotentialPartitions,
                                      partition_cnt);


        /* Sort valid partitions to minimize partition movements. */
        sortedPartitions = sortPartitions(&currentAssignment,
                                          &prevAssignment,
                                          isFreshAssignment,
                                          &partition2AllPotentialConsumers,
                                          &consumer2AllPotentialPartitions);


        /* All partitions that need to be assigned (initially set to all
         * partitions but adjusted in the following loop) */
        unassignedPartitions =
                rd_kafka_topic_partition_list_copy(sortedPartitions);

        RD_MAP_FOREACH(consumer, partitions, &currentAssignment) {
                if (!RD_MAP_GET(&subscriptions, consumer)) {
                        /* If a consumer that existed before
                         * (and had some partition assignments) is now removed,
                         * remove it from currentAssignment and its
                         * partitions from currentPartitionConsumer */

                        for (i = 0 ; i < partitions->cnt ; i++) {
                                const rd_kafka_topic_partition_t *partition =
                                        &partitions->elems[i];
                                RD_MAP_DELETE(&currentPartitionConsumer,
                                              partition);
                        }

                        /* FIXME: The delete could be optimized by passing the
                         *        underlying elem_t. */
                        RD_MAP_DELETE(&currentAssignment, consumer);

                } else {
                        /* Otherwise (the consumer still exists) */

                        for (i = 0 ; i < partitions->cnt ; i++) {
                                const rd_kafka_topic_partition_t *partition =
                                        &partitions->elems[i];
                                rd_bool_t remove_part = rd_false;

                                if (!RD_MAP_GET(
                                            &partition2AllPotentialConsumers,
                                            partition)) {
                                        /* If this partition of this consumer
                                         * no longer exists remove it from
                                         * currentAssignment of the consumer */
                                        remove_part = rd_true;
                                        RD_MAP_DELETE(&currentPartitionConsumer,
                                                      partition);

                                } else if (!rd_kafka_topic_partition_list_find(
                                                   RD_MAP_GET(&subscriptions,
                                                              consumer),
                                                   partition->topic,
                                                   RD_KAFKA_PARTITION_UA)) {
                                        /* If this partition cannot remain
                                         * assigned to its current consumer
                                         * because the consumer is no longer
                                         * subscribed to its topic, remove it
                                         * from the currentAssignment of the
                                         * consumer. */
                                        remove_part = rd_true;
                                        revocationRequired = rd_true;
                                } else {
                                        /* Otherwise, remove the topic partition
                                         * from those that need to be assigned
                                         * only if its current consumer is still
                                         * subscribed to its topic (because it
                                         * is already assigned and we would want
                                         * to preserve that assignment as much
                                         * as possible). */
                                        rd_kafka_topic_partition_list_del(
                                                unassignedPartitions,
                                                partition->topic,
                                                partition->partition);
                                }

                                if (remove_part) {
                                        rd_kafka_topic_partition_list_del_by_idx(
                                                partitions, i);
                                        i--; /* Since the current element was
                                              * removed we need the next for
                                              * loop iteration to stay at the
                                              * same index. */
                                }
                        }
                }
        }


        /* At this point we have preserved all valid topic partition to consumer
         * assignments and removed all invalid topic partitions and invalid
         * consumers.
         * Now we need to assign unassignedPartitions to consumers so that the
         * topic partition assignments are as balanced as possible. */

        /* An ascending sorted list of consumers based on how many topic
         * partitions are already assigned to them. The list element is
         * referencing the rd_map_elem_t* from the currentAssignment map. */
        rd_list_init(&sortedCurrentSubscriptions,
                     RD_MAP_CNT(&currentAssignment), NULL);

        RD_MAP_FOREACH_ELEM(elem, &currentAssignment.rmap)
                rd_list_add(&sortedCurrentSubscriptions, (void *)elem);

        rd_list_sort(&sortedCurrentSubscriptions,
                     sort_by_map_elem_val_toppar_list_cnt);

        /* Balance the available partitions across consumers */
        balance(rk,
                &partitionMovements,
                &currentAssignment,
                &prevAssignment,
                sortedPartitions,
                unassignedPartitions,
                &sortedCurrentSubscriptions,
                &consumer2AllPotentialPartitions,
                &partition2AllPotentialConsumers,
                &currentPartitionConsumer,
                revocationRequired);

        rd_list_destroy(&sortedCurrentSubscriptions);

        PartitionMovements_destroy(&partitionMovements);

        rd_kafka_topic_partition_list_destroy(unassignedPartitions);
        rd_kafka_topic_partition_list_destroy(sortedPartitions);

        RD_MAP_DESTROY(&currentPartitionConsumer);
        RD_MAP_DESTROY(&consumer2AllPotentialPartitions);
        RD_MAP_DESTROY(&partition2AllPotentialConsumers);
        RD_MAP_DESTROY(&prevAssignment);
        RD_MAP_DESTROY(&currentAssignment);
        RD_MAP_DESTROY(&subscriptions);

        return RD_KAFKA_RESP_ERR_NO_ERROR;
}


/**
 * @returns a score indicating how unbalanced the given assignment is, a higher
 *          higher score means the assignment is more unbalanced, while a score
 *          of 0 indicates a perfectly balanced assignment (each member
 *          has the same number of assigned partitions).
 */
