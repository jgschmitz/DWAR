# Stored Procedure → MongoDB Conversion

SQL stored procedures map to **aggregation pipelines** for read/transform logic, **Atlas Triggers + Functions** for event-driven or scheduled execution, and **multi-document transactions** for writes that must be atomic. No `CREATE PROCEDURE` syntax — the logic lives in your application layer or Atlas Functions.

---

## Table of Contents

- [Concept mapping](#concept-mapping)
- [SP 1: GetOpenGapsForMember](#sp-1-getopenGapsformember)
- [SP 2: SummarizeGapClosureByLOB](#sp-2-summarizegapclosurebylob)
- [SP 3: FlagHighRiskMembers (scheduled)](#sp-3-flaghighriskMembers-scheduled)
- [SP 4: CloseGapWithAudit (transactional write)](#sp-4-closegapwithaudit-transactional-write)
- [Index recommendations](#index-recommendations)

---

## Concept mapping

| SQL concept | MongoDB equivalent |
|---|---|
| `CREATE PROCEDURE` | Aggregation pipeline in app code or Atlas Function |
| `IN` parameter | Variable passed into `$match` stage |
| `SELECT … JOIN` | `$lookup` or pre-joined document model (preferred) |
| `GROUP BY` | `$group` stage |
| `ORDER BY` | `$sort` stage |
| `UPDATE … WHERE` | `updateMany` / `$merge` in pipeline |
| `TRANSACTION` | Multi-document transaction with `session` |
| Scheduled job | Atlas Scheduled Trigger |
| Event-driven trigger | Atlas Database Trigger |
| `OUTPUT` / result set | Pipeline result array or `$merge` into a collection |

---

## SP 1: GetOpenGapsForMember

**SQL equivalent**

```sql
CREATE PROCEDURE GetOpenGapsForMember
  @MbrId     VARCHAR(50),
  @SusYear   INT
AS
BEGIN
  SELECT
    m.MBR_ID,
    m.MBR_AGE,
    g.GAP_ID,
    g.GAP_DESC,
    g.GAP_STATUS,
    g.GAP_RNK,
    g.LIKELIHOOD,
    g.TTL_PROB
  FROM Members m
  JOIN Gaps g ON m.GLB_MBR_ID = g.GLB_MBR_ID
  WHERE m.MBR_ID     = @MbrId
    AND g.GAP_STATUS = 'OPEN'
    AND g.SUS_YEAR   = @SusYear
    AND g.ACTIVE_FLAG = 1
  ORDER BY g.GAP_RNK ASC
END
```

**MongoDB aggregation pipeline**

Because member and gap data are co-located in a single document (see the [schema overview](README.md#schema-overview)), no `$lookup` is needed.

```js
// mongosh
function getOpenGapsForMember(memberId, susYear) {
  return db.gaps.aggregate([
    {
      $match: {
        "member.memberId": memberId,
        "suspect.year":    susYear,
        "gap.status":      "OPEN",
        "status.active":   true
      }
    },
    {
      $project: {
        _id:              0,
        memberId:         "$member.memberId",
        age:              "$member.age",
        gapId:            "$gap.id",
        gapDescription:   "$gap.description",
        gapStatus:        "$gap.status",
        rank:             "$gap.rank",
        likelihood:       1,
        totalProbability: "$ttlProb"
      }
    },
    { $sort: { rank: 1 } }
  ]).toArray();
}

// call it
getOpenGapsForMember("M001", 2024);
```

Expected output shape:

```json
[
  {
    "memberId": "M001",
    "age": 67,
    "gapId": "GAP987",
    "gapDescription": "Statin therapy for diabetes",
    "gapStatus": "OPEN",
    "rank": 3,
    "likelihood": 0.74,
    "totalProbability": 0.80
  }
]
```

---

## SP 2: SummarizeGapClosureByLOB

Summarize how many gaps are open vs. closed, grouped by line of business and suspect year.

**SQL equivalent**

```sql
CREATE PROCEDURE SummarizeGapClosureByLOB
  @SusYear INT
AS
BEGIN
  SELECT
    c.METAL_LEVEL       AS LOB,
    g.GAP_STATUS,
    COUNT(*)            AS GapCount,
    AVG(g.LIKELIHOOD)   AS AvgLikelihood,
    AVG(g.TTL_PROB)     AS AvgTotalProb
  FROM Gaps g
  JOIN Coverage c ON g.GLB_MBR_ID = c.GLB_MBR_ID
  WHERE g.SUS_YEAR = @SusYear
  GROUP BY c.METAL_LEVEL, g.GAP_STATUS
  ORDER BY LOB, g.GAP_STATUS
END
```

**MongoDB aggregation pipeline**

```js
function summarizeGapClosureByLOB(susYear) {
  return db.gaps.aggregate([
    {
      $match: { "suspect.year": susYear }
    },
    {
      $group: {
        _id: {
          lob:       "$coverage.metalLevel",
          gapStatus: "$gap.status"
        },
        gapCount:       { $sum: 1 },
        avgLikelihood:  { $avg: "$likelihood" },
        avgTotalProb:   { $avg: "$ttlProb" }
      }
    },
    {
      $project: {
        _id:          0,
        lob:          "$_id.lob",
        gapStatus:    "$_id.gapStatus",
        gapCount:     1,
        avgLikelihood: { $round: ["$avgLikelihood", 2] },
        avgTotalProb:  { $round: ["$avgTotalProb", 2] }
      }
    },
    { $sort: { lob: 1, gapStatus: 1 } }
  ]).toArray();
}

summarizeGapClosureByLOB(2024);
```

Expected output shape:

```json
[
  { "lob": "GOLD",   "gapStatus": "CLOSED", "gapCount": 412, "avgLikelihood": 0.61, "avgTotalProb": 0.68 },
  { "lob": "GOLD",   "gapStatus": "OPEN",   "gapCount": 188, "avgLikelihood": 0.73, "avgTotalProb": 0.79 },
  { "lob": "SILVER", "gapStatus": "OPEN",   "gapCount": 304, "avgLikelihood": 0.69, "avgTotalProb": 0.74 }
]
```

---

## SP 3: FlagHighRiskMembers (scheduled)

A nightly job that finds members with `likelihood >= 0.80` and at least one open gap, then writes a summary into a `high_risk_members` collection.

**SQL equivalent**

```sql
-- typically a SQL Agent job calling a stored procedure
CREATE PROCEDURE FlagHighRiskMembers
  @SusYear INT
AS
BEGIN
  INSERT INTO HighRiskMembers (MBR_ID, OpenGapCount, MaxLikelihood, RunDate)
  SELECT
    GLB_MBR_ID,
    COUNT(*)         AS OpenGapCount,
    MAX(LIKELIHOOD)  AS MaxLikelihood,
    GETDATE()        AS RunDate
  FROM Gaps
  WHERE GAP_STATUS = 'OPEN'
    AND LIKELIHOOD >= 0.80
    AND SUS_YEAR = @SusYear
  GROUP BY GLB_MBR_ID
END
```

**MongoDB: aggregation pipeline with `$merge`**

Run this ad hoc or wire it into an Atlas Scheduled Trigger (see below).

```js
function flagHighRiskMembers(susYear) {
  db.gaps.aggregate([
    {
      $match: {
        "gap.status": "OPEN",
        "likelihood": { $gte: 0.80 },
        "suspect.year": susYear
      }
    },
    {
      $group: {
        _id:            "$member.globalId",
        openGapCount:   { $sum: 1 },
        maxLikelihood:  { $max: "$likelihood" },
        gapIds:         { $push: "$gap.id" }
      }
    },
    {
      $addFields: {
        memberId: "$_id",
        runDate:  "$$NOW",
        susYear:  susYear
      }
    },
    {
      $merge: {
        into:           "high_risk_members",
        on:             "_id",
        whenMatched:    "replace",
        whenNotMatched: "insert"
      }
    }
  ]);
}
```

**Atlas Scheduled Trigger (nightly at 02:00 UTC)**

In the Atlas UI: **Triggers → Add Trigger → Scheduled**

```js
// Atlas Function body
exports = async function() {
  const db      = context.services.get("mongodb-atlas").db("gaps_demo");
  const susYear = new Date().getFullYear();

  await db.collection("gaps").aggregate([
    {
      $match: {
        "gap.status": "OPEN",
        "likelihood": { $gte: 0.80 },
        "suspect.year": susYear
      }
    },
    {
      $group: {
        _id:           "$member.globalId",
        openGapCount:  { $sum: 1 },
        maxLikelihood: { $max: "$likelihood" },
        gapIds:        { $push: "$gap.id" }
      }
    },
    {
      $addFields: { runDate: "$$NOW", susYear }
    },
    {
      $merge: {
        into:           "high_risk_members",
        on:             "_id",
        whenMatched:    "replace",
        whenNotMatched: "insert"
      }
    }
  ]).toArray();

  console.log(`FlagHighRiskMembers complete for year ${susYear}`);
};
```

Schedule: `0 2 * * *` (cron, nightly at 02:00 UTC)

---

## SP 4: CloseGapWithAudit (transactional write)

Atomically mark a gap closed and append an audit entry — the multi-document equivalent of a SQL stored procedure that `UPDATE`s one table and `INSERT`s into an audit table within a transaction.

**SQL equivalent**

```sql
CREATE PROCEDURE CloseGapWithAudit
  @MbrId    VARCHAR(50),
  @GapId    VARCHAR(50),
  @ClosedBy VARCHAR(100),
  @Reason   VARCHAR(255)
AS
BEGIN
  BEGIN TRANSACTION
    UPDATE Gaps
    SET GAP_STATUS = 'CLOSED', STS_CHG_IND = 'Y'
    WHERE MBR_ID = @MbrId AND GAP_ID = @GapId

    INSERT INTO GapAudit (MBR_ID, GAP_ID, ACTION, CHANGED_BY, REASON, ACTION_DATE)
    VALUES (@MbrId, @GapId, 'CLOSE', @ClosedBy, @Reason, GETDATE())
  COMMIT TRANSACTION
END
```

**MongoDB multi-document transaction**

```js
async function closeGapWithAudit(memberId, gapId, closedBy, reason) {
  const client  = db.getMongo();
  const session = client.startSession();

  try {
    session.startTransaction();

    const gapsCol  = db.getSiblingDB("gaps_demo").gaps;
    const auditCol = db.getSiblingDB("gaps_demo").gap_audit;

    const result = gapsCol.updateOne(
      { "member.memberId": memberId, "gap.id": gapId },
      {
        $set: {
          "gap.status":        "CLOSED",
          "status.statusChangeInd": "Y"
        }
      },
      { session }
    );

    if (result.matchedCount === 0) {
      throw new Error(`Gap ${gapId} not found for member ${memberId}`);
    }

    auditCol.insertOne(
      {
        memberId,
        gapId,
        action:     "CLOSE",
        changedBy:  closedBy,
        reason,
        actionDate: new Date()
      },
      { session }
    );

    session.commitTransaction();
    print(`Gap ${gapId} closed and audit recorded.`);

  } catch (err) {
    session.abortTransaction();
    throw err;
  } finally {
    session.endSession();
  }
}

// call it
closeGapWithAudit("M001", "GAP987", "aliceInternal", "Provider confirmed statin started");
```

> **Note:** Multi-document transactions require a replica set or sharded cluster. A standalone `mongod` does not support them.

---

## Index recommendations

These indexes support the pipelines above. Add on top of the indexes in [README.md](README.md#indexes).

```text
{ "member.memberId": 1, "suspect.year": -1, "gap.status": 1 }   -- SP1
{ "coverage.metalLevel": 1, "suspect.year": -1, "gap.status": 1 } -- SP2
{ "likelihood": -1, "gap.status": 1, "suspect.year": -1 }        -- SP3
{ "member.memberId": 1, "gap.id": 1 }                            -- SP4
```
