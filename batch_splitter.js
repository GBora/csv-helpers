const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const readline = require('readline-sync');
const _ = require('lodash');

/*Parameters and other global data*/
let INPUT_FILE_NAME; // Name of the file to be split
let OUTPUT_FILE_NAME; // A template for the names of the files to be created
let ROWS_SKIP;
let BATCHES_NEEDED;
let BATCH_SIZE
let ROWS = [];
let HEADERS = [];

/* Helper functions */
setInputFileName = () => {
    if (process.argv[2]) {
        INPUT_FILE_NAME = process.argv[2];
        console.log(`Attempting to split file ${ INPUT_FILE_NAME }`);
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
    let ROWS_SKIP = readline.question(`How many rows to skip (0 by default) ? `) || 0;
}

calculateBatchesNumber = (rowsLength) => {
    let options = [2, 3, 4, 5, 6, 7, 8, 9, 10];
    options = options.filter(o => { return ((rowsLength % o) === 0) });
    console.log('Sugestion for number of batches: ');
    options.forEach(o => { console.log(o) });
    BATCHES_NEEDED = readline.question(`How many batches to prepare? `);
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
            if (ROWS_SKIP) {
                rows = ROWS.slice(ROWS_SKIP);
            } else {
                rows = ROWS;
            }
            calculateBatchesNumber(rows.length);
            // Making sure the batch size results in natural numbers
            BATCH_SIZE = Math.floor(rows.length / BATCHES_NEEDED);
            for (let i = 0; i < BATCHES_NEEDED; i++) {
                let begin = i * BATCH_SIZE;
                let end = begin + BATCH_SIZE;

                let fileName = `${ OUTPUT_FILE_NAME }_${ begin + 1 }-${ end }`;

                createOuputFile(fileName, HEADERS, rows.slice(begin, end))
            }            
        });
}

/* Script main */
setInputFileName();
setOutputFileName();
setRowsToSkip();
parseFile();
