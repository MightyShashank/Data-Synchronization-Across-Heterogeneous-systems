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
            const key = `${student_id}_${course_id}`;
            let vector_clock;

            try {
                vector_clock = JSON.parse(vector_clock_str);
            } catch (e) {
                console.error(`Invalid vector clock JSON: ${vector_clock_str}`);
                continue;
            }

            const currentMerged = mergedVectorClocks.get(key) || {};
            for (const node in vector_clock) {
                currentMerged[node] = (currentMerged[node] || 0) + vector_clock[node];
            }
            mergedVectorClocks.set(key, currentMerged);
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


// new update for postgres

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

            const currentMerged = mergedVectorClocks.get(key) || {};
            for (const node in vector_clock) {
                currentMerged[node] = (currentMerged[node] || 0) + vector_clock[node];
            }
            mergedVectorClocks.set(key, currentMerged);
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

// new update for hive

async function updateOriginalDBUsingCacheDB(tsvFilePath = path.join(__dirname, 'hive_cachelog.tsv')) {
    const client = new hive.HiveClient(TCLIService, TCLIService_types);
    const utils = new hive.HiveUtils(TCLIService_types);

    const hiveClient = await client.connect(
        { host: '127.0.0.1', port: 10000 },
        new hive.connections.TcpConnection(),
        new hive.auth.PlainTcpAuthentication({
            username: 'hive',
            password: 'hive'
        })
    );

    const session = await hiveClient.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
    });

    console.log('Connected to Hive Original Database!');
    const data = fs.readFileSync(tsvFilePath, 'utf8');
    const lines = data.trim().split('\n').filter(line => line.trim() !== '');

    // First Pass: Build merged vector clocks
    const mergedVectorClocks = new Map();

    for (const line of lines) {
        const [operation, , vector_clock_str, , student_id, course_id] = line.split('\t');
        if (!line.trim()) continue;

        if (operation !== 'set') continue;
        const key = `${student_id}_${course_id}`;

        let vector_clock;
        try {
            vector_clock = JSON.parse(vector_clock_str);
        } catch (err) {
            console.error(`Invalid vector clock: ${vector_clock_str}`);
            continue;
        }

        const currentMerged = mergedVectorClocks.get(key) || {};
        for (const node in vector_clock) {
            currentMerged[node] = (currentMerged[node] || 0) + vector_clock[node];
        }
        mergedVectorClocks.set(key, currentMerged);
    }

    // Second Pass: Update Hive using only 'set' operations
    for (const line of lines) {
        const [operation, , , time_range, student_id, course_id, grade] = line.split('\t');
        if (!line.trim() || operation !== 'set') continue;

        const key = `${student_id}_${course_id}`;
        const mergedClock = JSON.stringify(mergedVectorClocks.get(key));
        const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);

        const query = `
            INSERT OVERWRITE TABLE original_table_hive
            SELECT 
                CASE WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${mergedClock}' ELSE vector_clock END AS vector_clock,
                CASE WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${lower_bound_utc}' ELSE lower_bound_utc END AS lower_bound_utc,
                CASE WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${upper_bound_utc}' ELSE upper_bound_utc END AS upper_bound_utc,
                student_id,
                course_id,
                CASE WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${grade}' ELSE grade END AS grade
            FROM original_table_hive
        `;

        try {
            const op = await session.executeStatement(query);
            await utils.waitUntilReady(op, false, () => {});
            await op.close();
            console.log(`Updated Hive for ${student_id}, ${course_id}`);
        } catch (err) {
            console.error('Error executing Hive query:', err);
        }
    }

    await session.close();
    await hiveClient.close();
    console.log('Hive sync with merged vector clocks completed!');
}


// mongo old update

async function updateOriginalDBUsingCacheDB(tsvFilePath = path.join(__dirname, 'postgresql_cachelog.tsv')) {
    
    const client = new Client({connectionString: connectionString});

    try {
        await client.connect();
        console.log('Connected to PostgreSQL Original Database!');

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

            if(operation !== 'set') continue; // we're skipping 'get' operations

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
                vector_clock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                gradeRaw
            ];

            await client.query(query, values);
            
            console.log('Updated DB');
        }

        console.log('Database sync completed!')

    }
    catch (err) {
        console.error('Error: ', err);
    }
    finally {
        await client.end();
    }
}


// update old postgres

async function updateOriginalDBUsingCacheDB(tsvFilePath = path.join(__dirname, 'postgresql_cachelog.tsv')) {
    
    const client = new Client({connectionString: connectionString});

    try {
        await client.connect();
        console.log('Connected to PostgreSQL Original Database!');

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

            if(operation !== 'set') continue; // we're skipping 'get' operations

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
                vector_clock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                gradeRaw
            ];

            await client.query(query, values);
            
            console.log('Updated DB');
        }

        console.log('Database sync completed!')

    }
    catch (err) {
        console.error('Error: ', err);
    }
    finally {
        await client.end();
    }
}

// update old hive

async function updateOriginalDBUsingCacheDB(tsvFilePath = path.join(__dirname, 'hive_cachelog.tsv')) {

    const client = new hive.HiveClient(TCLIService, TCLIService_types);
    const utils = new hive.HiveUtils(TCLIService_types);

    const hiveClient = await client.connect(
        {host:'127.0.0.1', port:10000},
        new hive.connections.TcpConnection(),
        new hive.auth.PlainTcpAuthentication({
            username: 'hive', 
            password: 'hive'  
        })
    );

    const session = await hiveClient.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
    });

    console.log('Connected to Hive Original Database!');
    const data = fs.readFileSync(tsvFilePath, 'utf8');
    const lines = data.trim().split('\n').filter(line => line.trim() !== '');

    for (const line of lines) {

        // Check if line is not empty or undefined
        if (!line.trim()) {
            console.log('Skipping empty line...');
            continue;  // Skip empty lines
        }
        const [operation, source, vector_clock, time_range, student_id, course_id, grade] = line.split('\t');
        const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);
        if (operation !== 'set') continue;

        const query = `
            INSERT OVERWRITE TABLE original_table_hive
            SELECT
                vector_clock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                CASE 
                    WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${grade}'
                    ELSE grade
                END AS grade
            FROM original_table_hive
        `;

        const op = await session.executeStatement(query);
        await utils.waitUntilReady(op, false, () => {});
        await op.close();
        console.log('Updated DB');
    }

    await session.close();
    await hiveClient.close();
}
