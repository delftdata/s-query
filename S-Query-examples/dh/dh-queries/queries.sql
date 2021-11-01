
-- Select by delivery zone
SELECT COUNT(*), deliveryZone FROM "snapshot_orderinfo" GROUP BY deliveryZone;

-- Group by order state
SELECT COUNT(*), orderState FROM "snapshot_orderstate" GROUP BY orderState;

-- 1 How many deliveries per area are late (are in preparation phase more than 10 minutes)
SELECT COUNT(*), deliveryZone FROM "snapshot_orderinfo" t1 JOIN "snapshot_orderstate" t2 USING(partitionKey) WHERE orderState='VENDOR_ACCEPTED' AND lateTimestamp<LOCALTIMESTAMP GROUP BY deliveryZone;

-- 2 How many deliveries are available per shop category?
SELECT COUNT(*), vendorCategory FROM "snapshot_orderinfo" t1 JOIN "snapshot_orderstate" t2 USING(partitionKey) WHERE orderState='NOTIFIED' OR orderState='ACCEPTED' GROUP BY vendorCategory;

-- 3 How many deliveries are being prepared per area?
SELECT COUNT(*), deliveryZone FROM "snapshot_orderinfo" t1 JOIN "snapshot_orderstate" t2 USING(partitionKey) WHERE orderState='VENDOR_ACCEPTED' GROUP BY deliveryZone;

-- 4 How many deliveries are in transit per area?
SELECT COUNT(*), deliveryZone FROM "snapshot_orderinfo" t1 JOIN "snapshot_orderstate" t2 USING(partitionKey) WHERE orderState='PICKED_UP' OR orderState='LEFT_PICKUP' OR orderState='NEAR_CUSTOMER' GROUP BY deliveryZone;

-- Count incremental snapshots per key
SELECT partitionKey, COUNT(*) AS ssCount FROM "snapshot_orderstate" GROUP BY partitionKey ORDER BY ssCount, partitionKey;

-- Get latest snapshot ID per key
SELECT t1.partitionKey, MAX(snapshotId) FROM "snapshot_orderstate" t1 GROUP BY partitionKey;

-- Naive way of getting latest snapshot id, caveat is that it can miss keys with only 1 snapshot
SELECT t1.* FROM "snapshot_orderstate" t1 LEFT JOIN "snapshot_orderstate" t2 ON (CAST(t1.partitionKey AS INT)=CAST(t2.partitionKey AS INT) AND t1.snapshotId > t2.snapshotId AND t1.snapshotId <= 940);

-- Latest incremental snapshot with subquery (partly limited latest query), edge case is that rows with max > latest snapshot id are not selected
SELECT a.* FROM "snapshot_orderstate" a INNER JOIN (SELECT partitionKey, MAX(snapshotId) AS snapshotId FROM "snapshot_orderstate" GROUP BY partitionKey HAVING MAX(snapshotId) < 2000) b ON (CAST(a.partitionKey AS INT)=CAST(b.partitionKey AS INT) AND a.snapshotId = b.snapshotId);

-- Latest incremental snapshot, fully consistent with latest snapshot ID (replace 2500 with latest snapshot id)
SELECT a.* FROM "snapshot_orderstate" a INNER JOIN (SELECT c.partitionKey, MAX(c.snapshotId) AS maxSnapshotId FROM "snapshot_orderstate" c WHERE c.snapshotId <= 2500 GROUP BY c.partitionKey) b ON (CAST(a.partitionKey AS INT)=CAST(b.partitionKey AS INT) AND a.snapshotId = b.maxSnapshotId);
