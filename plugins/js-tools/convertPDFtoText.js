const fs = require("fs");
const { Poppler } = require("node-poppler");
const poppler = new Poppler("/usr/bin");

const file = process.argv[2]
const start = parseInt(process.argv[3]); // First argument
const end = parseInt(process.argv[4]); // Second argument

const options = {
    maintainLayout: true,
    noPageBreaks: true,
    firstPageToConvert: start,
    lastPageToConvert: end
};

let txt_file_name = file.replace('.pdf','.txt')

console.log(file,txt_file_name,options)

try {
    poppler.pdfToText(file,txt_file_name , options).then((res) => {
        console.log(res);
    }).catch((error) => {
        console.error('Error during conversion:', error);
    });
} catch (error) {
    console.error('Unexpected error:', error);
}