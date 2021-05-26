function csvToJSON(csvString) {

    var lines = csvString.split("\r");

    var result = [];

    var headers = lines[0].split(",");

    for (var i = 1; i < lines.length; i++) {

        var obj = {};
        var currentline = lines[i].split(",");
        for (var j = 0; j < headers.length; j++) {
            obj[headers[j]] = currentline[j];
        }

        result.push(obj);

    }

    result.forEach((element, index) => {
        element.sampleNumber = parseInt(element.sampleNumber);
        element.clumpThickness = parseInt(element.clumpThickness);
        element.cellSize = parseInt(element.cellSize);
        element.cellShape = parseInt(element.cellShape);
        element.adhesion = parseInt(element.adhesion);
        element.singleCellSize = parseInt(element.singleCellSize);
        element.nuclei = parseInt(element.nuclei);
        element.chromatin = parseInt(element.chromatin);
		element.normalNucleoli = parseInt(element.normalNucleoli);
        element.mitoses = parseInt(element.mitoses);
        element.class = parseInt(element.class);
    });

    return result; 
}

const Helpers = {
    csvToJSON: csvToJSON
}
module.exports = {
    Helpers: Helpers
}