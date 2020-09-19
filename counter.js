const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const readline = require('readline-sync');
const _ = require('lodash');

/*Parameters and other global data*/
let INPUT_FILE_NAME; // Name of the file to be split
let OUTPUT_FILE_NAME; // A template for the names of the files to be created
let ROWS_SKIP;
let ROWS = [];
let HEADERS = [];
let COUNTER = [];

/* Helper functions */
setInputFileName = () => {
    if (process.argv[2]) {
        INPUT_FILE_NAME = process.argv[2];
        console.log(`Attempting to read file ${ INPUT_FILE_NAME }`);
    } else {
        console.error(`Input filename is mandatory please provide as first parameter !`);
        process.exit();
    }
}

setOutputFileName = () => {
    while (!OUTPUT_FILE_NAME) {
        OUTPUT_FILE_NAME = readline.question(`What should be the name of the output files? `);
    }
}

setRowsToSkip = () => {
    ROWS_SKIP = readline.question(`How many rows to skip (0 by default) ? `) || 0;
}

createOuputFile = (filename, headers, data) => {
    let completeFilename = filename + '.csv';
    const csvWriter = createCsvWriter({
        path: completeFilename,
        header: headers
    });
      
    //TODO handle error
    csvWriter
        .writeRecords(data)
        .then(()=> console.log(`Finished writting ${completeFilename}`));
}

counterFunction = (row) => {
    // Just an example this should have much more advanced logic within
    if (row['header_1'] === '111') {
        return true;
    }
    return false;
}

parseFile = () => {
    fs.createReadStream(INPUT_FILE_NAME)
        .pipe(csv())
        .on('headers', (headers) => {
            headers.forEach((h) => {
                HEADERS.push({id: h, title: h})
            })
        })
        .on('data', (row) => {
            ROWS.push(row);
        })
        .on('end', () => {
            console.log(`Finished reading ${ INPUT_FILE_NAME } which contains ${ ROWS.length } rows`);
            let rows;
            let selectedRows = [];
            if (ROWS_SKIP) {
                rows = ROWS.slice(ROWS_SKIP);
            } else {
                rows = ROWS;
            }
            selectedRows = rows.filter(row => counterFunction(row));
            COUNTER = selectedRows.length;
            console.log(`Rows that match the criteria: ${ COUNTER } `);    
        });
}

/* Script main */
setInputFileName();
// setOutputFileName();
setRowsToSkip();
parseFile();
