const hive = require('hive-driver');
const { TCLIService, TCLIService_types } = hive.thrift;

const client = new hive.HiveClient(TCLIService, TCLIService_types);
const utils = new hive.HiveUtils(
    TCLIService_types
);

client.connect(
    {
        host: '127.0.0.1',
        port: 10000,
    },
    new hive.connections.TcpConnection(),
    new hive.auth.PlainTcpAuthentication({
        username: 'hive', // or use any dummy user like 'user'
        password: 'hive'  // or empty string ''
    })
).then(async client => {
    console.log("started");
    const session = await client.openSession({
        client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
    });
//
    console.log("Here");
    
    const createTableOperation = await session.executeStatement(
        'CREATE TABLE IF NOT EXISTS sometestTable (foo INT, bar STRING)'
    );
    await utils.waitUntilReady(createTableOperation, false, () => {});
    await createTableOperation.close();

    await session.close();
    await client.close();
    console.log("Done! table created");
}).catch(error => {
    console.error('Error connecting to Hive:', error);
});

//////////////////////////////////////////////////

async function pushCacheLogToOpLogDB(tsvFilePath = path.join(__dirname, 'hive_cachelog.tsv')) {
    
    // Initialize PostgreSQL client
    console.log("Hello world");
    client.connect(
        {host:'127.0.0.1', port:10000},
        new hive.connections.TcpConnection(),
        new hive.auth.PlainTcpAuthentication({
            username: 'hive', 
            password: 'hive'  
        })
    ).then(async client => {
        try {
            const session = await client.openSession({
                client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
            });
            console.log('Connected to Hive Oplog!');
    
            // Read the TSV file
            const data = fs.readFileSync(tsvFilePath, 'utf8');
            const lines = data.trim().split('\n');
    
            lines.forEach(line=>console.log(line));
            for (const line of lines) {
                const [operation, source, vector_clock, time_range, student_id, course_id, gradeRaw] = line.split('\t');
    
                const { lower_bound_utc, upper_bound_utc } = JSON.parse(time_range);
    
                // If operation is "get", set grade to null
                const grade = operation === 'get' ? null : gradeRaw;
    
                // Insert into your oplog table
                const query = `
                    INSERT INTO oplog_hive (source, operation, vector_clock, lower_bound_utc, upper_bound_utc, student_id, course_id, grade)
                    VALUES ('${source}', '${operation}', '${vector_clock}', '${lower_bound_utc}', '${upper_bound_utc}', '${student_id}', '${course_id}', '${grade}')
                `;
    
                // const insertData = await session
                const loadGradeOperation = await session.executeStatement(query);
                await utils.waitUntilReady(loadGradeOperation, false, () => {});
                await loadGradeOperation.close();
                console.log('Inserted');
            }
    
            await session.close();
            await client.close();
        }
        catch (err) {
            console.error('Error: ', err);
        }
        finally {
            // await client.end();
        }
    })
    .catch(error => {console.error(error)});  
}


////////////////////////////////////////////////////////////////////////////////////////////////////////

async function set(student_id, course_id, grade) {
    // set the grade


    // Initialize PostgreSQL client
    console.log("Hello world");
    client.connect(
        {host:'127.0.0.1', port:10000},
        new hive.connections.TcpConnection(),
        new hive.auth.PlainTcpAuthentication({
            username: 'hive', 
            password: 'hive'  
        })
    ).then(async client => {
        const session = await client.openSession({
            client_protocol: TCLIService_types.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10
        });
        console.log('Connected to Hive Oplog!');

        // now get the vector clock
        const getVectorClockQuery = `
            SELECT vector_clock FROM original_table_hive
            WHERE student_id = '${student_id.replace(/'/g, "''")}' AND course_id = '${course_id.replace(/'/g, "''")}'
        `;

        const loadVectorOperation = await session.executeStatement(getVectorClockQuery);
        await utils.waitUntilReady(loadVectorOperation, false, ()=>{});
        await utils.fetchAll(loadVectorOperation);
        const result = utils.getResult(loadVectorOperation).getValue();
        await loadVectorOperation.close();

        let vector_clock = result[0].vector_clock;
        console.log('vector_clock before fixing = ', vector_clock);
        // Fix the missing quote before 'postgresql'
        if (!vector_clock.trim().startsWith('{"')) {
            vector_clock = vector_clock.replace('{postgresql', '{"postgresql');
        }

        console.log('vector_clock after fixing = ', vector_clock);
        let vectorClockJSON = JSON.parse(vector_clock);
        console.log("Before incrementing:", vectorClockJSON);
        vectorClockJSON.hive = (vectorClockJSON.hive || 0) + 1;
        console.log("After incrementing:", vectorClockJSON);

        const query = `
            -- Update the grade for student_id = 'SID1033' and course_id = 'CSE016' to 'D'
            INSERT OVERWRITE TABLE original_table_hive
            SELECT
                CASE 
                    WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${JSON.stringify(vectorClockJSON)}'
                    ELSE vector_clock
                END AS vector_clock,
                lower_bound_utc,
                upper_bound_utc,
                student_id,
                course_id,
                CASE 
                    WHEN student_id = '${student_id}' AND course_id = '${course_id}' THEN '${grade}'  -- Update grade to 'D'
                    ELSE grade  -- Keep the original grade for other records
                END AS grade
            FROM original_table_hive
        `;

        // const insertData = await session
        const loadGradeOperation = await session.executeStatement(query);
        await utils.waitUntilReady(loadGradeOperation, false, () => {});
        await loadGradeOperation.close();
        console.log('Updated DB');

        

        appendToCacheLog({
            operation: "set",
            vector_clock: vectorClockJSON,
            student_id: student_id,
            course_id: course_id,
            grade: grade
        });

        await session.close();
        await client.close();
    })
    .catch(error => {console.error(error)});
    /////////////////// Stop
}