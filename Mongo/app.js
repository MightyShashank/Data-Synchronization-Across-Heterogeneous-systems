const express = require('express');
const axios = require('axios');
const readline = require('readline');
const mongoose = require('mongoose');
const { MongoClient } = require('mongodb');

// to extract the trueTime
async function fetchNplTimeRange() {
    try {

        // hitting the TrueTimeUTCserver.py
        const response = await fetch('http://127.0.0.1:5000/get-time');
        
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const data = await response.json();
        const {lower_bound_utc, upper_bound_utc } = data.time_range;
        return [lower_bound_utc, upper_bound_utc]
        // from this extract the key "adjusted_utc_time" 
        
    } catch (error) {
        console.error("Failed to fetch time:", error);
    }
}

// // Call the function
// fetchNplTimeRange();

// Above we return a range: so see for [lower_bound_utc, upper_bound_utc]
// {
//     "original_utc_time": ntp_datetime.strftime("%Y-%m-%d %H:%M:%S"),
//     "timestamp": ntp_time,
//     "latency_ms": latency * 1000,
//     "time_range": {
//         "lower_bound_timestamp": lower_bound,
//         "upper_bound_timestamp": upper_bound,
//         "lower_bound_utc": datetime.utcfromtimestamp(lower_bound).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
//         "upper_bound_utc": datetime.utcfromtimestamp(upper_bound).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
//     }
// }


// Lets create our cachelog file first of and everything

const fs = require('fs');
const path = require('path');

// Now lets initialise our postgresql database 
 

// PostgreSQL connection string
const connectionString = 'mongodb+srv://AppuAmazonSambhav:1234@shiplinkobjectcluster.e4q75.mongodb.net/mydatabase';

const source = "mongo"

// Function to push cache log to psql database
async function appendToCacheLog({ operation, vector_clock, student_id, course_id, grade }) {

    const logFilePath = path.join(__dirname, 'mongo_cachelog.tsv');
    
    if(operation === 'set') {
        const [lower_bound_utc, upper_bound_utc] = await fetchNplTimeRange();

        const time_stamp = JSON.stringify({
            lower_bound_utc: lower_bound_utc,
            upper_bound_utc: upper_bound_utc
        });

        // Prepare the line (each field separated by tab '\t')
        const logEntry = `\n${operation}\t${source}\t${JSON.stringify(vector_clock)}\t${time_stamp}\t${student_id}\t${course_id}\t${grade}\n`;
        
        // Append the entry to the file
        fs.appendFile(logFilePath, logEntry, (err) => {
            if (err) {
                console.error('Error writing to cachelog file:', err);
            } 
            else {
                console.log('Successfully appended to cachelog.');
            }
        });
    }

    else if(operation === 'get') {
        
        const [lower_bound_utc, upper_bound_utc] = await fetchNplTimeRange();

        const time_stamp = JSON.stringify({
            lower_bound_utc: lower_bound_utc,
            upper_bound_utc: upper_bound_utc
        });

        const logEntry = `\n${operation}\t${source}\t${JSON.stringify(vector_clock)}\t${time_stamp}\t${student_id}\t${course_id}\t${'undefined'}\n`;

        // Append the entry to the file
        fs.appendFile(logFilePath, logEntry, (err) => {
            if (err) {
                console.error('Error writing to cachelog file:', err);
            } 
            else {
                console.log('Successfully appended to cachelog.');
            }
        });
    }
}

/*
// Example usage
appendToCacheLog({
  vector_clock: { postgresql: 1, hive: 3, mongo: 2 },   // JSON Object
  time_stamp: new Date().toISOString(),  // Current timestamp in ISO format
  student_id: 'S123',
  course_id: 'C456',
  grade: 'A'
});
*/


// Helper to parse a line into an object
function parseLine(line) {
  const [operation, source, vectorClockStr, timeRangeStr, student_id, course_id, grade] = line.split('\t');
  const vector_clock = JSON.parse(vectorClockStr);

  const timeRange = JSON.parse(timeRangeStr); // {lower_bound_utc: "", upper_bound_utc: ""}
  return {
    operation,
    source,
    vector_clock,
    lower_bound_utc: timeRange.lower_bound_utc,
    upper_bound_utc: timeRange.upper_bound_utc,
    student_id,
    course_id,
    grade
  };
}

// Helper to compare vector clocks
function compareVectorClocks(vc1, vc2) {
  let vc1Greater = false;
  let vc2Greater = false;

  const keys = new Set([...Object.keys(vc1), ...Object.keys(vc2)]); // collecting all keys present in both of them 
  for (const key of keys) { // for each key check its value in vc1 and vc2
    const val1 = vc1[key] || 0; // if key is missing we treat it as 0
    const val2 = vc2[key] || 0;

    if (val1 > val2) vc1Greater = true;
    if (val2 > val1) vc2Greater = true;
  }

  // ideally we want one of them to be true and one of them to be false
  if (vc1Greater && !vc2Greater) return 1;  // vc1 dominates 
  if (vc2Greater && !vc1Greater) return -1; // vc2 dominates
  return 0; // concurrent
}

// Helper to compare time ranges
function compareUpperBounds(t1, t2) {
  const date1 = new Date(t1);
  const date2 = new Date(t2);

  if (date1 > date2) return 1;
  if (date2 > date1) return -1;
  return 0;
}

// Save the final merged output into a new TSV file, utility function
function saveMergedResult(entries, outputPath) {
    const lines = entries.map(entry => {
        const vectorClockStr = JSON.stringify(entry.vector_clock);
        const timeRangeStr = JSON.stringify({
            lower_bound_utc: entry.lower_bound_utc,
            upper_bound_utc: entry.upper_bound_utc
        });
        return `${entry.operation}\t${entry.source}\t${vectorClockStr}\t${timeRangeStr}\t${entry.student_id}\t${entry.course_id}\t${entry.grade}`;
    });
  
    fs.writeFileSync(outputPath, lines.join('\n'), 'utf8'); // this overwrites the file by default if it exists
    console.log("Succesfully overwritten\n")
}

function fullSortByVectorClockAndTime(arr) {
    arr.sort((a, b) => {
    const vcComp = compareVectorClocks(a.vector_clock, b.vector_clock);
    if (vcComp !== 0) return vcComp;

    // If vector clocks are concurrent, fall back to upper_bound_utc
    return compareUpperBounds(a.upper_bound_utc, b.upper_bound_utc);
    });
}


function mergeCacheLogAndOverwriteCurrentCacheLog(tsvPath1, tsvPath2) { // working well, tested

    const file1 = fs.readFileSync(tsvPath1, 'utf8').trim().split('\n').filter(line=>line.trim() !== '').map(parseLine); // we also removed empty lines
    const file2 = fs.readFileSync(tsvPath2, 'utf8').trim().split('\n').filter(line=>line.trim() !== '').map(parseLine); // we also removed empty lines

    // Convert entries to JSON strings for comparison
    const file1Set = new Set(file1.map(e => JSON.stringify(e)));
    const file2Set = new Set(file2.map(e => JSON.stringify(e)));

    //checking if all lines of file2 are already in file1
    // Check if all lines in file2 are already in file1
    const file2FullyContained = file2.every(e => file1Set.has(JSON.stringify(e)));

    if (file2FullyContained) {
        console.log("Files were already merged, nothing to merge");
        return;
    }
    // Filter file1 to exclude entries also present in file2
    const uniqueFile1 = file1.filter(e => !file2Set.has(JSON.stringify(e)));

    // Combine unique entries only
    let allEntries = [...uniqueFile1, ...file2]; // combining entries of file1 and file2

    // Sort by student_id then course_id
    allEntries.sort((a, b) => {
        if (a.student_id !== b.student_id) {
            return a.student_id.localeCompare(b.student_id);
        }
            return a.course_id.localeCompare(b.course_id);
    });

    // console.log("After sorting = ", allEntries) // working 

    // Now resolve conflicts
    let result = [];
    let finalResult = []
    let i = 0;
    while (i < allEntries.length) {
        const current = allEntries[i];
        const duplicates = [current];

        let j = i + 1;
        while (j < allEntries.length && allEntries[j].student_id === current.student_id && allEntries[j].course_id === current.course_id) {
            // console.log("Hellow we are the same")
            duplicates.push(allEntries[j]);
            j++;
        }

        // If only one entry, no conflict
        if (duplicates.length === 1) {
            result.push(current);
            // console.log("result for dup - 1 = ", result)
        } 
        else {
            // Resolve conflicts among duplicates
            // console.log("duplicates = ", duplicates); // fine only
            fullSortByVectorClockAndTime(duplicates);
            result.push(...duplicates);
            // console.log("result = ", result);
        }

        i = j;
    }
    finalResult.push(...result);
    // console.log("final result = ", finalResult)
    saveMergedResult(finalResult, path.join(__dirname, 'mongo_cachelog.tsv'));
}

// this pushes in the order in which events were supposed to happen
async function pushCacheLogToOpLogDB(tsvFilePath = path.join(__dirname, 'mongo_cachelog.tsv')) {

  const client = new MongoClient(connectionString);

  try {
      await client.connect();
      console.log('Connected to Mongo Oplog!');

      const db = client.db('mydatabase');  // Replace with your database name
      const collection_oplog = db.collection('oplog_mongo');  // Replace with your oplog collection name

      // Read the TSV file
      const data = fs.readFileSync(tsvFilePath, 'utf8');
      const lines = data.trim().split('\n');

      for (const line of lines) {

           // Check if line is not empty or undefined
          if (!line.trim()) {
            console.log('Skipping empty line...');
            continue;  // Skip empty lines
          }
          const [operation, source, vector_clock, time_range, student_id, course_id, gradeRaw] = line.split('\t');

          // console.log(JSON.parse(time_range));
          const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);

          // If operation is "get", set grade to null
          const grade = operation === 'get' ? null : gradeRaw;

          // Create the document to be inserted into the oplog collection
          const document = {
              source,
              operation,
              vector_clock,
              lower_bound_utc,
              upper_bound_utc,
              student_id,
              course_id,
              grade,
          };

          // Insert the document into the oplog collection
          try {
              const result = await collection_oplog.insertOne(document);
              console.log('Inserted into oplog:', result.insertedId);
          } catch (err) {
              console.error('Error inserting into oplog:', err);
          }
      }
  } catch (err) {
      console.error('Error:', err);
  } finally {
      await client.close();
      console.log('MongoDB client connection closed.');
  }
}

async function updateOriginalDBUsingCacheDB(tsvFilePath = path.join(__dirname, 'mongo_cachelog.tsv')) {
  const client = new MongoClient(connectionString);

  try {
      await client.connect();
      console.log('Connected to Mongo Original Database!');
      const db = client.db('mydatabase');
      const collection = db.collection('original_table_mongo');

      // Read and split lines
      const data = fs.readFileSync(tsvFilePath, 'utf8');
      const lines = data.trim().split('\n');

      // First pass: accumulate merged vector clocks
      const mergedVectorClocks = new Map();

      for (const line of lines) {
          if (!line.trim()) continue;
      
          const [operation, , vector_clock_str, , student_id, course_id] = line.split('\t');
          if (operation !== 'set') continue;
      
          const key = `${student_id}_${course_id}`;
      
          let vector_clock;
          try {
              vector_clock = JSON.parse(vector_clock_str);
          } catch (err) {
              console.error(`Invalid vector clock: ${vector_clock_str}`);
              continue;
          }
      
          const currentMerged = mergedVectorClocks.get(key);
          
          if (!currentMerged) {
              mergedVectorClocks.set(key, vector_clock);
          } else {
              const comparison = compareVectorClocks(currentMerged, vector_clock);
              if (comparison === -1) {
                  // New vector_clock dominates
                  mergedVectorClocks.set(key, vector_clock);
              }
              // Else: current dominates or concurrent, so keep existing
          }
      }

      // Second pass: update DB using merged vector clocks for only 'set' ops
      for (const line of lines) {
          if (!line.trim()) continue;

          const [operation, , , time_range, student_id, course_id, gradeRaw] = line.split('\t');
          if (operation !== 'set') continue;

          const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);
          const key = `${student_id}_${course_id}`;
          const mergedVectorClock = mergedVectorClocks.get(key);

          const filter = { student_id, course_id };
          const update = {
              $set: {
                  vector_clock: mergedVectorClock,
                  lower_bound_utc,
                  upper_bound_utc,
                  grade: gradeRaw,
              },
          };

          try {
              const result = await collection.updateOne(filter, update, { upsert: true });
              if (result.upsertedCount > 0) {
                  console.log(`Inserted new document for student ${student_id} and course ${course_id}`);
              } else {
                  console.log(`Updated document for student ${student_id} and course ${course_id}`);
              }
          } catch (err) {
              console.error('Error updating document:', err);
          }
      }

      console.log('Database sync with fully merged vector clocks completed!');
  } catch (err) {
      console.error('Connection Error:', err);
  } finally {
      await client.close();
  }
}


async function fetchCacheFileContent(url) {

    try {
        const response = await axios.get(url);
        return response.data; // This is our cache file content
    }
    catch (error) {
        console.error('Error fetching file:', error);
    }
}

async function merge(system1 = 'mongo', system2) {


    try {
        const tsvFilePath1 = `${system1}_cachelog.tsv`
        const tsvFilePath2 = `${system2}_cachelog.tsv`

        const tsvCacheLogFile1 = path.join(__dirname, tsvFilePath1);
        const tsvCacheLogFile2 = path.join(__dirname, tsvFilePath2);

        // just populate this tsvCacheLogFile2, its supposed to be a local copy of the cache file just copied.... // but after merge be sure to clear these files

        if(system2 === 'hive') {

            // writing cache content to local file system
            const content = await fetchCacheFileContent('http://localhost:3001/hive_cachelog');
            fs.writeFileSync(tsvCacheLogFile2, content, 'utf8');
        }
        else if (system2 === 'postgresql') {
            
            const content = await fetchCacheFileContent('http://localhost:4001/postgresql_cachelog');
            fs.writeFileSync(tsvCacheLogFile2, content, 'utf8')
        }

        // now you are supposed to merge the cache log files 
        mergeCacheLogAndOverwriteCurrentCacheLog(tsvCacheLogFile1, tsvCacheLogFile2);

        // now push them to Oplog DB of current postgres
        await pushCacheLogToOpLogDB();

        // now update our current database using our current merged cachelog 
        await updateOriginalDBUsingCacheDB();

        console.log("Merge of cachelog successful, pushed the cachelog to oplog db, updated the original postgres database")
        return 0;
    }
    catch (error) {
        console.error('Error is merge function ', error);
        return 1;
    }
    finally {

        const tsvFilePath2 = `${system2}_cachelog.tsv`
        const tsvCacheLogFile2 = path.join(__dirname, tsvFilePath2);

        // just clearing our imported cache log files once their work is done
        fs.writeFileSync(tsvCacheLogFile2, '', 'utf-8');
    }
}

async function get(student_id, course_id) {

  const client = new MongoClient(connectionString);

  try {
      await client.connect();
      console.log('Connected to Mongo Original Database to get data!');

      const db = client.db('mydatabase');  // Replace with your database name
      const collection = db.collection('original_table_mongo');  // Replace with your collection name

      // Query to fetch the grade and vector clock for the given student_id and course_id
      const query = { student_id, course_id };
      const projection = { grade: 1, vector_clock: 1 };  // Only fetch grade and vector_clock fields
      const result = await collection.findOne(query, { projection });

      if (result) {
          console.log("Successfully fetched data = ", result.grade);
          console.log(result.vector_clock);
          // Append to cache log
          await appendToCacheLog({
              operation: "get",
              vector_clock: result.vector_clock,  // JSON Object
              student_id: student_id,
              course_id: course_id
          });

          return result.grade;  // Return the grade
      } else {
          console.log("No data found for the given student_id and course_id");
          return null;
      }
  } catch (err) {
      console.error('Error:', err);
  } finally {
      await client.close();
  }
}

async function set(student_id, course_id, grade) {

  const client = new MongoClient(connectionString);

  try {
      await client.connect();
      console.log('Connected to Mongo Original Database to set data!');

      const db = client.db('mydatabase');  // Replace with your database name
      const collection = db.collection('original_table_mongo');  // Replace with your collection name

      // Fetch the current vector clock for the given student_id and course_id
      const query = { student_id, course_id };
      const projection = { vector_clock: 1 };  // Only fetch vector_clock
      const result = await collection.findOne(query, { projection });

      if (result) {
          const vector_clock = result.vector_clock || { mongo: 0 };

          // console.log("Before incrementing:", vector_clock);
          vector_clock.mongo = (vector_clock.mongo || 0) + 1;  // Increment the vector clock
          // console.log("After incrementing:", vector_clock);

          // Update the grade and vector clock in the database
          const updateResult = await collection.updateOne(
              { student_id, course_id },
              {
                  $set: {
                      grade: grade,
                      vector_clock: vector_clock
                  }
              }
          );

          if (updateResult.modifiedCount > 0) {
              console.log('Successfully updated grade and vector clock!');
          } else {
              console.log('No document was updated.');
          }

          // Append the operation to the cache log
          await appendToCacheLog({
              operation: "set",
              vector_clock: vector_clock,  // JSON Object
              student_id: student_id,
              course_id: course_id,
              grade: grade
          });
      } else {
          console.log("No data found for the given student_id and course_id.");
      }
  } catch (err) {
      console.error('Error:', err);
  } finally {
      await client.close();
  }
}

async function initializeDB(csvPath) {
  const client = new MongoClient(connectionString);

  try {
    await client.connect();
    console.log('Connected to MongoDB Original Database!');

    const db = client.db('mydatabase');  // Replace with your database name
    
    // Initialize collections (this ensures that the collections exist)
    const collection = await db.createCollection('original_table_mongo', { capped: false });
    const collection_oplog = await db.createCollection('oplog_mongo', { capped: false });

    // delete whatever was already present
    await db.collection('original_table_mongo').deleteMany({});
    await db.collection('oplog_mongo').deleteMany({});

    console.log('Collections initialized (or already exist): original_table_mongo, oplog_mongo');

    const data = fs.readFileSync(csvPath, 'utf8');
    const lines = data.trim().split('\n');

    const [lower, upper] = await fetchNplTimeRange();
    if (!lower || !upper) {
      console.error("Failed to hit time server!");
      return;
    }

    // Iterate through each line in the CSV and insert documents into MongoDB
    for (const line of lines) {
      const [student_id, course_id, grade] = line.split(',');

      const document = {
        vector_clock: { postgresql: 0, hive: 0, mongo: 0 },
        lower_bound_utc: lower,
        upper_bound_utc: upper,
        student_id: student_id,
        course_id: course_id,
        grade: grade
      };

      // Insert into the 'original_table_mongo' collection
      try {
        const result = await collection.insertOne(document);
        console.log(`Inserted into original_table_mongo: ${student_id}, ${course_id}, ${grade}`);
      } catch (err) {
        console.error('Error inserting into original_table_mongo:', err);
      }
    }
  } catch (err) {
    console.error('Error connecting to MongoDB:', err);
  } finally {
    await client.close();
    console.log('Database initialization complete.');
  }
}


// initializeDB(path.join(__dirname, 'student_course_grades.csv'));

// set('SID1469','CSE006', 'C');
// set('SID1033', 'CSE016', 'C');
// set('SID1033', 'CSE016', 'D');
// get('SID1033', 'CSE016');
// mergeAndResolve(
//     path.join(__dirname, 'file1.tsv'),
//     path.join(__dirname, 'file2.tsv')
// );
  

// Example usage
// appendToCacheLog({
//   operation: "get",
//   vector_clock: { postgresql: 3, hive: 4, mongo: 2 },   // JSON Object
//   student_id: 'SID1069',
//   course_id: 'CSE069' 
// });


// mergeCacheLogAndOverwriteCurrentCacheLog('postgresql_cachelog.tsv','postgresql_cachelog.tsv')



// pushCacheLogToOpLogDB();

// updateOriginalDBUsingCacheDB();

// (async () => {
//   try {
//       // await get('SID1734', 'CSE013');
//       await get('SID1965', 'CSE004');
//   } 
//   catch (err) {
//       console.log("Error during main operations set and get: ", err);
//   }
// })();

// (async () => {
//   try {
//       merge('mongo', 'hive');
//   } 
//   catch (err) {
//       console.log("Error during main operations set and get or merge: ", err);
//   }
// })();

module.exports = {
  merge,
  set,
  get
};

// merge('mongo', 'hive')
// set('SID1033',	'CSE016', 'C');