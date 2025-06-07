const express = require('express');
const path = require('path');
const fs = require('fs')

const app = express();
const PORT = 5001;

app.get('/mongo_cachelog', (req, res) => {

    const filePath = path.join(__dirname, 'mongo_cachelog.tsv'); 

    fs.readFile(filePath, 'utf8', (err, data) => {
        if(err) {
            return res.status(500).send('Error reading hive cachelog file');
        }
        res.send(data); // send the current contents of th file
    });
})

app.listen(PORT, () => {
    console.log(`Hive Cache log server running at http://localhost:${PORT}/mongo_cachelog`);
})