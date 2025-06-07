const hive = require('./Hive/app');
const mongo = require('./Mongo/app');
const postgresql = require('./PostgreSQL/app');
const fs = require('fs');
const path = require('path');

const input = `
HIVE.SET((SID1965,CSE004),A) 
MONGO.SET((SID1965, CSE004),B)
SQL.SET((SID1965, CSE004),C)
HIVE.MERGE(MONGO) 
SQL.MERGE(HIVE) 
HIVE.GET(SID1965,CSE004) 
MONGO.GET(SID1965, CSE004)
SQL.GET(SID1965, CSE004)
`;

// Synchronize option for synchronizing all databases (we then clear cache)
async function Synchronize() {
    
    try {
        await hive.merge('hive', 'mongo');
        await hive.merge('hive', 'postgresql');
        console.log("Hive database synced");

        await postgresql.merge('postgresql', 'hive');
        await postgresql.merge('postgresql', 'mongo');
        console.log("Postgresql database synced");

        await mongo.merge('mongo', 'hive');
        await mongo.merge('mongo', 'postgresql');
        console.log("Mongo database synced");

    }
    catch(err) {
        console.error(err);
    }
    finally {
        // 
        console.log('All databases synced upto data!');

        // Clear the cachelogs

        // clear the postgresql_cachelog.tsv
        const postgresCacheLog = path.join(__dirname, 'PostgreSQL', 'postgresql_cachelog.tsv');
        fs.writeFile(postgresCacheLog, '', (err) => {
            if(err) {
                console.error('Error clearing cache file postgresql_cachelog: ', err);
            }
            else {
                console.log('postgresql_cachelog cleared successfully');
            }
        });

        // clear the hive_cachelog.tsv
        const hiveCacheLog = path.join(__dirname, 'Hive', 'hive_cachelog.tsv');
        fs.writeFile(hiveCacheLog, '', (err) => {
            if(err) {
                console.error('Error clearing cache file hive_cachelog: ', err);
            }
            else {
                console.log('hive_cachelog cleared successfully');
            }
        });

        // clear the mongo_cachelog.tsv
        const mongoCacheLog = path.join(__dirname, 'Mongo', 'mongo_cachelog.tsv');
        fs.writeFile(mongoCacheLog, '', (err) => {
            if(err) {
                console.error('Error clearing cache file mongo_cachelog: ', err);
            }
            else {
                console.log('mongo_cachelog cleared successfully');
            }
        });

        console.log("All cachelogs successfully cleared");
    }
}

const dbMap = {
  HIVE: hive,
  SQL: postgresql,
  MONGO: mongo,
};

const dbNameMap = {
  HIVE: 'hive',
  SQL: 'postgresql',
  MONGO: 'mongo',
};

const parsedCalls = [];

const lines = input.trim().split('\n');

for (const line of lines) {
  const trimmed = line.trim().replace(/\s+/g, '');
//   console.log('trimmed = ', trimmed);

  const matchSet = trimmed.match(/^(\w+)\.SET\(\(\s*(SID\d+),\s*(CSE\d+)\s*\),\s*([A-Z])\s*\)$/);
  const matchGet = trimmed.match(/^(\w+)\.GET\(\s*(SID\d+),\s*(CSE\d+)\s*\)$/);
  const matchMerge = trimmed.match(/^(\w+)\.MERGE\(\s*(\w+)\s*\)$/);

//   console.log('matchset = ', matchSet);
//   console.log('matchget = ', matchGet);
//   console.log('matchmerge = ', matchMerge);

  if (matchSet) {
    const [_, db, sid, cid, grade] = matchSet;
    parsedCalls.push({
      fn: () => dbMap[db].set(sid, cid, grade),
      log: `${dbNameMap[db]}.set('${sid}', '${cid}', '${grade}');`
    });
  } else if (matchGet) {
    const [_, db, sid, cid] = matchGet;
    parsedCalls.push({
      fn: () => dbMap[db].get(sid, cid),
      log: `${dbNameMap[db]}.get('${sid}', '${cid}');`
    });
  } else if (matchMerge) {
    const [_, db1, db2] = matchMerge;
    parsedCalls.push({
      fn: () => dbMap[db1].merge(dbNameMap[db1], dbNameMap[db2]),
      log: `${dbNameMap[db1]}.merge('${dbNameMap[db1]}', '${dbNameMap[db2]}');`
    });
  } else {
    parsedCalls.push({ log: `// Could not parse line: ${line}`, fn: async () => {} });
  }
}

(async () => {
  // Print all parsed calls
  for (const { log } of parsedCalls) {
    console.log(log);
  }

  console.log('\n--- Executing ---\n');

  // Execute all parsed calls sequentially
  for (const { fn } of parsedCalls) {
    await fn();
  }

//   await Synchronize();
})();


// (async () => {
//     await Synchronize();
// })();