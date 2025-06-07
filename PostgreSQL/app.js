const express = require('express');
const axios = require('axios');
const readline = require('readline');

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

const { Client } = require('pg'); // a named export 

// PostgreSQL connection string
const connectionString = 'postgresql://shashank:shashank@localhost:5433/mydatabase';

const source = "postgresql"

// Function to push cache log to psql database
async function appendToCacheLog({ operation, vector_clock, student_id, course_id, grade }) {

    const logFilePath = path.join(__dirname, 'postgresql_cachelog.tsv');
    
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
    saveMergedResult(finalResult, path.join(__dirname, 'postgresql_cachelog.tsv'));
}

// this pushes in the order in which events were supposed to happen
async function pushCacheLogToOpLogDB(tsvFilePath = path.join(__dirname, 'postgresql_cachelog.tsv')) {
    
    // Initialize PostgreSQL client
    const client = new Client({connectionString: connectionString});

    try {
        await client.connect();
        console.log('Connected to PostgreSQL Oplog!');

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

            const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);

            // If operation is "get", set grade to null
            const grade = operation === 'get' ? null : gradeRaw;

            // Insert into your oplog table
            const query = `
                INSERT INTO oplog_psql (source, operation, vector_clock, lower_bound_utc, upper_bound_utc, student_id, course_id, grade)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            `;
            const values = [
                source,
                operation,
                vector_clock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                grade
            ];

            await client.query(query, values);
            console.log('Inserted into oplog');
        }
    }
    catch (err) {
        console.error('Error: ', err);
    }
    finally {
        await client.end();
    }
}

async function updateOriginalDBUsingCacheDB(tsvFilePath = path.join(__dirname, 'postgresql_cachelog.tsv')) {
    const client = new Client({ connectionString });

    try {
        await client.connect();
        console.log('Connected to PostgreSQL Original Database!');

        const data = fs.readFileSync(tsvFilePath, 'utf8');
        const lines = data.trim().split('\n');

        // First Pass: Merge vector clocks
        const mergedVectorClocks = new Map();

        for (const line of lines) {
            if (!line.trim()) continue;
        
            const [operation, , vector_clock_str, , student_id, course_id] = line.split('\t');
            const key = `${student_id}_${course_id}`;
        
            let vector_clock;
            try {
                vector_clock = JSON.parse(vector_clock_str);
            } catch (e) {
                console.error(`Invalid vector clock JSON: ${vector_clock_str}`);
                continue;
            }
        
            const existingVC = mergedVectorClocks.get(key);
        
            if (!existingVC) {
                // First vector clock for this student-course
                mergedVectorClocks.set(key, vector_clock);
            } else {
                const comp = compareVectorClocks(existingVC, vector_clock);
                if (comp === -1) {
                    // Replace only if the new vector clock dominates
                    mergedVectorClocks.set(key, vector_clock);
                }
                // If comp === 1 or 0, we keep the existing one (dominates or concurrent)
            }
        }

        // Second Pass: Apply only 'set' operations using merged vector clocks
        for (const line of lines) {
            if (!line.trim()) continue;

            const [operation, , , time_range, student_id, course_id, gradeRaw] = line.split('\t');
            if (operation !== 'set') continue;

            const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);
            const key = `${student_id}_${course_id}`;
            const mergedVectorClock = JSON.stringify(mergedVectorClocks.get(key));

            const query = `
                INSERT INTO original_table_psql (vector_clock, lower_bound_utc, upper_bound_utc, student_id, course_id, grade)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (student_id, course_id)
                DO UPDATE SET 
                    grade = EXCLUDED.grade,
                    vector_clock = EXCLUDED.vector_clock,
                    lower_bound_utc = EXCLUDED.lower_bound_utc,
                    upper_bound_utc = EXCLUDED.upper_bound_utc;
            `;

            const values = [
                mergedVectorClock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                gradeRaw,
            ];

            try {
                await client.query(query, values);
                console.log(`Updated DB for ${student_id}, ${course_id}`);
            } catch (err) {
                console.error('Error updating DB:', err);
            }
        }

        console.log('PostgreSQL sync with merged vector clocks completed!');
    } catch (err) {
        console.error('Connection Error:', err);
    } finally {
        await client.end();
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

async function merge(system1 = 'postgresql', system2) {


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
        else if (system2 === 'mongo') {
            
            const content = await fetchCacheFileContent('http://localhost:5001/mongo_cachelog');
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
    // you are supposed to return the grade here

    const client = new Client({connectionString: connectionString});

    try {
        await client.connect();
        console.log('Connected to PostgreSQL Original Database to get data!');

        const query = `
            SELECT grade FROM original_table_psql
            WHERE student_id = $1 AND course_id = $2;
        `;
        const values = [
            student_id,
            course_id
        ];
        const result = await client.query(query, values);

        // also get current vector clock cause you anyways dont update that fort hat row
        const getVectorClockQuery = `
            SELECT vector_clock FROM original_table_psql
            WHERE student_id = $1 AND course_id = $2;
        `;
        const vector_clock_raw = await client.query(getVectorClockQuery, values); // our current vector clock
        const vector_clock = vector_clock_raw.rows[0].vector_clock;

        // console.log("vector_clock = ", vector_clock);
        if(result.rows.length>0) {
            console.log("Succesfully fetched data = ", result.rows[0].grade);

            // now you also need to write it to cache file
            await appendToCacheLog({
                operation: "get",
                vector_clock: vector_clock,   // JSON Object
                student_id: student_id,
                course_id: course_id
            });
            return result.rows[0].grade; // return the grade
        }
        else {
            console.log("No data found for the give student_id and course_id");
            return null;
        }
    }
    catch (err) {
        console.error('Error: ', err);
        // throw err; 
    }
    finally {
        await client.end();
    }
}

async function set(student_id, course_id, grade) {
    // set the grade

    const client = new Client({connectionString: connectionString});

    try {
        await client.connect();
        console.log('Connected to PostgreSQL Original Database to set data!');

        const getVectorClockQuery = `
            SELECT vector_clock FROM original_table_psql
            WHERE student_id = $1 AND course_id = $2;
        `;
        const newValues = [
            student_id,
            course_id
        ]
         // now you also need to increment the vector clock
        const vector_clock_raw = await client.query(getVectorClockQuery, newValues); // our current vector clock
        const vector_clock = vector_clock_raw.rows[0].vector_clock;

        // console.log("Before incrementing:", vector_clock);
        vector_clock.postgresql = (vector_clock.postgresql || 0) + 1;
        // console.log("After incrementing:", vector_clock);


        const query = `
            UPDATE original_table_psql
            SET grade = $1, vector_clock = $2 
            WHERE student_id = $3 AND course_id = $4;
        `;
        const values = [
            grade,
            vector_clock,
            student_id,
            course_id
        ];
        const result = await client.query(query, values);
       
        await appendToCacheLog({
            operation: "set",
            vector_clock: vector_clock,   // JSON Object
            student_id: student_id,
            course_id: course_id,
            grade: grade
        });
        
    }
    catch (err) {
        console.error('Error: ', err)
        // throw err;
    }
    finally {
        await client.end();
    }
}


async function initializeDB(csvPath) {
    const client = new Client({ connectionString });
    await client.connect();
    console.log('Connected to PostgreSQL Original Database!');

    const data = fs.readFileSync(csvPath, 'utf8');
    const lines = data.trim().split('\n');

    const [lower, upper] = await fetchNplTimeRange();
    if (!lower || !upper) {
        console.error("failed to hit time server!");
    }

    for (const line of lines) {
        const [student_id, course_id, grade] = line.split(',');

        const query = `
            INSERT INTO original_table_psql (
                vector_clock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                grade
            )
            VALUES ($1, $2, $3, $4, $5, $6);
        `;
        const values = [
            { postgresql: 0, hive: 0, mongo: 0 },
            lower,
            upper,
            student_id,
            course_id,
            grade
        ];

        await client.query(query, values);
        console.log(`Inserted: ${student_id}, ${course_id}, ${grade}`);
    }

    await client.end();
    console.log('Database initialization complete.');
}

// initializeDB(path.join(__dirname, 'student_course_grades.csv'));

// set('SID1469','CSE006', 'C');
// get('SID1469', 'CSE006');

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

// merge('postgresql', 'hive');

// (async () => {
//     try {
//         // merge('postgresql', 'hive');
//         merge('postgresql', 'mongo');
//     } 
//     catch (err) {
//         console.log("Error during main operations set and get or merge: ", err);
//     }
// })();

// (async () => {
//     try {
//         await get('SID1965', 'CSE016');
//         await get('SID1115', 'CSE009');
//         await get('SID1734', 'CSE013');
//     } 
//     catch (err) {
//         console.log("Error during main operations set and get or merge: ", err);
//     }
// })();

module.exports = {
    merge,
    set,
    get
};
