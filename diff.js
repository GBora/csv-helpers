const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const readline = require('readline-sync');
const _ = require('lodash');

/*Parameters and other global data*/
let INPUT_FILE_NAME;
let INPUT2_FILE_NAME; 
let OUTPUT_FILE_NAME; 
let ROWS = [];
let ROWS2 = [];
let ROWs_DIFF = [];
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

setComparisonFileName = () => {
    if (process.argv[3]) {
        INPUT2_FILE_NAME = process.argv[3];
        console.log(`Will compare with ${ INPUT_FILE_NAME }`);
    } else {
        console.error(`Please provide name of second file to compare with !`);
        process.exit();
    }
}

setOutputFileName = () => {
    while (!OUTPUT_FILE_NAME) {
        OUTPUT_FILE_NAME = readline.question(`What should be the name of the output files? `);
    }
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
            // console.log(row)
            ROWS.push(row);
        })
        .on('end', () => {
            console.log(`Finished reading ${ INPUT_FILE_NAME } which contains ${ ROWS.length } rows`);
            fs.createReadStream(INPUT2_FILE_NAME)
            .pipe(csv())
            .on('data', (row) => {
                ROWS2.push(row);
            })
            .on('end', () => {
                ROWS.forEach( (initRow, index) => {
                    console.log(`Progress ${ ((index * 100) / ROWS.length) }%`)
                    if (!_.find(ROWS2, { '_id': initRow['_id'] })) {
                        ROWs_DIFF.push(initRow);
                        console.log('ROW MISSING');
                    }
                })
                createOuputFile('diff', HEADERS, ROWs_DIFF);
            })        
        });
}

/* Script main */
setInputFileName();
setComparisonFileName();
// setOutputFileName();
// setRowsToSkip();
parseFile();
