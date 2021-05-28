const fs = require("fs");
const { MongoClient } = require("mongodb");
const { Parser } = require("./Parser");

const databaseUri = "mongodb://localhost:27017";
const client = new MongoClient(databaseUri, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

const main = async () => {

	let unsuportedValuesCount = {
		sampleNumber: 0,
		clumpThickness: 0,
		cellSize: 0,
		cellShape: 0,
		adhesion: 0,
		singleCellSize: 0,
		nuclei: 0,
		chromatin: 0,
		normalNucleoli: 0,
		mitoses: 0
	}

	let statistika1_breastCancer = {
		sampleNumber: [],
		clumpThickness: [],
		cellSize: [],
		cellShape: [],
		adhesion: [],
		singleCellSize: [],
		nuclei: [],
		chromatin: [],
		normalNucleoli: [],
		mitoses: []
	};
	
	let statistika2_breastCancer = {
		sampleNumber: [],
		clumpThickness: [],
		cellSize: [],
		cellShape: [],
		adhesion: [],
		singleCellSize: [],
		nuclei: [],
		chromatin: [],
		normalNucleoli: [],
		mitoses: []
	};
		
    try {
        await client.connect();
        try{
            await client.db("nosqlbreastCancer").dropDatabase();
        }
        catch(err){
        }

        const database = client.db("nosqlbreastCancer");
        const mongoDb = database.collection("breastCancerData");
        
        

        const csvString = fs.readFileSync("./dataSetAll.csv", "utf-8");

        let parsedCsvObject = Parser.csvToJSON(csvString);

        // 1. Zadatak
        // 2. zadatak, treći dio
		for(let i=0; i < parsedCsvObject.length; i++){
            if (parsedCsvObject[i].sampleNumber === null || parsedCsvObject[i].sampleNumber === undefined || parsedCsvObject[i].sampleNumber === "" || isNaN(parsedCsvObject[i].sampleNumber)) {
                unsuportedValuesCount.sampleNumber++;
                parsedCsvObject[i].sampleNumber = -1;
            }
            if (parsedCsvObject[i].clumpThickness === null || parsedCsvObject[i].clumpThickness === undefined || parsedCsvObject[i].clumpThickness === "" || isNaN(parsedCsvObject[i].clumpThickness)) {
                unsuportedValuesCount.clumpThickness++;
                parsedCsvObject[i].clumpThickness = -1;
            }
            if (parsedCsvObject[i].cellSize === null || parsedCsvObject[i].cellSize === undefined || parsedCsvObject[i].cellSize === "" || isNaN(parsedCsvObject[i].cellSize)) {
                unsuportedValuesCount.cellSize++;
                parsedCsvObject[i].cellSize = -1;
            }
            if (parsedCsvObject[i].cellShape === null || parsedCsvObject[i].cellShape === undefined || parsedCsvObject[i].cellShape === "" || isNaN(parsedCsvObject[i].cellShape)) {
                unsuportedValuesCount.cellShape++;
                parsedCsvObject[i].cellShape = -1;
            }
            if (parsedCsvObject[i].adhesion === null || parsedCsvObject[i].adhesion === undefined || parsedCsvObject[i].adhesion === "" || isNaN(parsedCsvObject[i].adhesion)) {
                unsuportedValuesCount.adhesion++;
                parsedCsvObject[i].adhesion = -1;
            }
            if (parsedCsvObject[i].singleCellSize === null || parsedCsvObject[i].singleCellSize === undefined || parsedCsvObject[i].singleCellSize === "" || isNaN(parsedCsvObject[i].singleCellSize)) {
                unsuportedValuesCount.singleCellSize++;
                parsedCsvObject[i].singleCellSize = -1;
            }
            if (parsedCsvObject[i].nuclei === null || parsedCsvObject[i].nuclei === undefined || parsedCsvObject[i].nuclei === "" || isNaN(parsedCsvObject[i].nuclei)) {
                unsuportedValuesCount.nuclei++;
                parsedCsvObject[i].nuclei = -1;
            }
            if (parsedCsvObject[i].chromatin === null || parsedCsvObject[i].chromatin === undefined || parsedCsvObject[i].chromatin === "" || isNaN(parsedCsvObject[i].chromatin)) {
                unsuportedValuesCount.chromatin++;
                parsedCsvObject[i].chromatin = -1;
            }
			if (parsedCsvObject[i].normalNucleoli === null || parsedCsvObject[i].normalNucleoli === undefined || parsedCsvObject[i].normalNucleoli === "" || isNaN(parsedCsvObject[i].normalNucleoli)) {
                unsuportedValuesCount.normalNucleoli++;
                parsedCsvObject[i].normalNucleoli = -1;
            }
			if (parsedCsvObject[i].mitoses === null || parsedCsvObject[i].mitoses === undefined || parsedCsvObject[i].mitoses === "" || isNaN(parsedCsvObject[i].mitoses)) {
                unsuportedValuesCount.mitoses++;
                parsedCsvObject[i].mitoses = -1;
            }
            if (parsedCsvObject[i].class === null || parsedCsvObject[i].class === undefined || parsedCsvObject[i].class === "" || isNaN(parsedCsvObject[i].class)) {
                parsedCsvObject[i].class = "empty";
            }
        };

        await mongoDb.insertMany(parsedCsvObject);

        // 2. zadatak, prvi dio 
        const averageValues = await mongoDb.aggregate(
            [
                {
                    $group: {
                        _id: null,
                        Average_sampleNumber: {"$avg": '$sampleNumber'},
                        Average_clumpThickness: {"$avg":"$clumpThickness"},
                        Average_cellSize: {"$avg":"$cellSize"},
                        Average_cellShape: {"$avg":"$cellShape"},
                        Average_adhesion: {"$avg":"$adhesion"},
                        Average_singleCellSize: {"$avg":"$singleCellSize"},
                        Average_nuclei: {"$avg":"$nuclei"},
                        Average_chromatin: {"$avg":"$chromatin"},
						Average_normalNucleoli: {"$avg":"$normalNucleoli"},
						Average_mitoses: {"$avg":"$mitoses"}
                    }
                }
            ]
        );

        // 2. zadatak, drugi dio 
        const standardDeviation = await mongoDb.aggregate(
            [
                {
                    $group: {
                        _id: null,
                        StdDevSamp_sampleNumber: {"$stdDevSamp": '$sampleNumber'},
                        StdDevSamp_clumpThickness: {"$stdDevSamp":"$clumpThickness"},
                        StdDevSamp_cellSize: {"$stdDevSamp":"$cellSize"},
                        StdDevSamp_cellShape: {"$stdDevSamp":"$cellShape"},
                        StdDevSamp_adhesion: {"$stdDevSamp":"$adhesion"},
                        StdDevSamp_singleCellSize: {"$stdDevSamp":"$singleCellSize"},
                        StdDevSamp_nuclei: {"$stdDevSamp":"$nuclei"},
                        StdDevSamp_chromatin: {"$stdDevSamp":"$chromatin"},
						StdDevSamp_normalNucleoli: {"$stdDevSamp":"$normalNucleoli"},
						StdDevSamp_mitoses: {"$stdDevSamp":"$mitoses"}
                    }
                }
            ]
        );

        const breastCancerStatistic = {
            averageStatisticValues: (await averageValues.toArray())[0],
            averageStandardDeviationValues: (await standardDeviation.toArray())[0]
        };
        
        const breastCancerStatisticCollection = database.collection("statistika_breastCancer");
        await breastCancerStatisticCollection.insertOne(breastCancerStatistic);

        //2. zadatak, treći dio
        const suportedValuesCount = {
            sampleNumber: parsedCsvObject.length - unsuportedValuesCount.sampleNumber,
            clumpThickness: parsedCsvObject.length - unsuportedValuesCount.clumpThickness,
            cellSize: parsedCsvObject.length - unsuportedValuesCount.cellSize,
            cellShape: parsedCsvObject.length - unsuportedValuesCount.cellShape,
            adhesion: parsedCsvObject.length - unsuportedValuesCount.adhesion,
            singleCellSize: parsedCsvObject.length - unsuportedValuesCount.singleCellSize,
            nuclei: parsedCsvObject.length - unsuportedValuesCount.nuclei,
            chromatin: parsedCsvObject.length - unsuportedValuesCount.chromatin,
			normalNucleoli: parsedCsvObject.length - unsuportedValuesCount.normalNucleoli,
			mitoses: parsedCsvObject.length - unsuportedValuesCount.mitoses
        };



        // 3. Zadatak
        const benignFrequency = await mongoDb.aggregate(
            [
                {
                    $match:{
                        class: 2
                    }
                },
                {
                    $count: "benign"
                }
            ]
        );

        const malignantFrequency = await mongoDb.aggregate(
            [
                {
                    $match:{
                        class: 4
                    }
                },
                {
                    $count: "malignant"
                }
            ]
        );

        let breastCancerFrequency = [];
        breastCancerFrequency.push((await benignFrequency.toArray())[0]);
        breastCancerFrequency.push((await malignantFrequency.toArray())[0]);

        const frequencyCollection = database.collection("frekvencija_breastCancer");
        await frequencyCollection.insertMany(breastCancerFrequency);

        // 4. zadatak

	for(let i=0; i < parsedCsvObject.length; i++){
            // sampleNumber
            if(parsedCsvObject[i].sampleNumber <= breastCancerStatistic.averageStatisticValues.Average_sampleNumber){
                statistika1_breastCancer.sampleNumber.push(parsedCsvObject[i].sampleNumber);
            }
            else{
                statistika2_breastCancer.sampleNumber.push(parsedCsvObject[i].sampleNumber);
            }

            // clumpThickness
            if(parsedCsvObject[i].clumpThickness <= breastCancerStatistic.averageStatisticValues.Average_clumpThickness){
                statistika1_breastCancer.clumpThickness.push(parsedCsvObject[i].clumpThickness);
            }
            else{
                statistika2_breastCancer.clumpThickness.push(parsedCsvObject[i].clumpThickness);
            }

            // cellSize
            if(parsedCsvObject[i].cellSize <= breastCancerStatistic.averageStatisticValues.Average_cellSize){
                statistika1_breastCancer.cellSize.push(parsedCsvObject[i].cellSize);
            }
            else{
                statistika2_breastCancer.cellSize.push(parsedCsvObject[i].cellSize);
            }

            // cellShape
            if(parsedCsvObject[i].cellShape <= breastCancerStatistic.averageStatisticValues.Average_cellShape){
                statistika1_breastCancer.cellShape.push(parsedCsvObject[i].cellShape);
            }
            else{
                statistika2_breastCancer.cellShape.push(parsedCsvObject[i].cellShape);
            }

            // adhesion
            if(parsedCsvObject[i].adhesion <= breastCancerStatistic.averageStatisticValues.Average_adhesion){
                statistika1_breastCancer.adhesion.push(parsedCsvObject[i].adhesion);
            }
            else{
                statistika2_breastCancer.adhesion.push(parsedCsvObject[i].adhesion);
            }

            // singleCellSize
            if(parsedCsvObject[i].singleCellSize <= breastCancerStatistic.averageStatisticValues.Average_singleCellSize){
                statistika1_breastCancer.singleCellSize.push(parsedCsvObject[i].singleCellSize);
            }
            else{
                statistika2_breastCancer.singleCellSize.push(parsedCsvObject[i].singleCellSize);
            }

            // nuclei
            if(parsedCsvObject[i].nuclei <= breastCancerStatistic.averageStatisticValues.Average_nuclei){
                statistika1_breastCancer.nuclei.push(parsedCsvObject[i].nuclei);
            }
            else{
                statistika2_breastCancer.nuclei.push(parsedCsvObject[i].nuclei);
            }

            // chromatin
            if(parsedCsvObject[i].chromatin <= breastCancerStatistic.averageStatisticValues.Average_chromatin){
                statistika1_breastCancer.chromatin.push(parsedCsvObject[i].chromatin);
            }
            else{
                statistika2_breastCancer.chromatin.push(parsedCsvObject[i].chromatin);
            }
			
			if(parsedCsvObject[i].normalNucleoli <= breastCancerStatistic.averageStatisticValues.Average_normalNucleoli){
                statistika1_breastCancer.normalNucleoli.push(parsedCsvObject[i].normalNucleoli);
            }
            else{
                statistika2_breastCancer.normalNucleoli.push(parsedCsvObject[i].normalNucleoli);
            }
			
			if(parsedCsvObject[i].mitoses <= breastCancerStatistic.averageStatisticValues.Average_mitoses){
                statistika1_breastCancer.mitoses.push(parsedCsvObject[i].mitoses);
            }
            else{
                statistika2_breastCancer.mitoses.push(parsedCsvObject[i].mitoses);
            }
        };

        const statistika1Collection = database.collection("statistika1_breastCancer");
        await statistika1Collection.insertOne(statistika1_breastCancer);

        const statistika2Collection = database.collection("statistika2_breastCancer");
        await statistika2Collection.insertOne(statistika2_breastCancer);


        // 5. zadatak 
        let emb_breastCancer = await mongoDb.find().toArray();
    
		for(let i=0; i < emb_breastCancer.length; i++){
            let tempclassVal = emb_breastCancer[i].class;

            //vrijednost moze biti samo 0 ili 1 za property "class"
            if(emb_breastCancer[i].class == 2){
                emb_breastCancer[i].class = {
                    value: tempclassVal,
                    frequency_emb: breastCancerFrequency[0]._id
                }
            }
            else {
                emb_breastCancer[i].class = {
                    value: tempclassVal,
                    frequency: breastCancerFrequency[1]._id
                }
            }
        };

        const embCollection = database.collection("emb_breastCancer");
        await embCollection.insertMany(emb_breastCancer);


        // 6. zadatak
        let emb2_breastCancer = await mongoDb.find().toArray();    

		for(let i=0; i < emb2_breastCancer.length; i++){
            emb2_breastCancer[i].sampleNumber = {
                value: emb2_breastCancer[i].sampleNumber,
                statistics: breastCancerStatistic._id
            };

            emb2_breastCancer[i].clumpThickness = {
                value: emb2_breastCancer[i].clumpThickness,
                statistics: breastCancerStatistic._id
            }

            emb2_breastCancer[i].cellSize = {
                value: emb2_breastCancer[i].cellSize,
                statistics: breastCancerStatistic._id
            }

            emb2_breastCancer[i].cellShape = {
                value: emb2_breastCancer[i].cellShape,
                statistics: breastCancerStatistic._id
            }

            emb2_breastCancer[i].adhesion = {
                value: emb2_breastCancer[i].adhesion,
                statistics: breastCancerStatistic._id
            }

            emb2_breastCancer[i].singleCellSize = {
                value: emb2_breastCancer[i].singleCellSize,
                statistics: breastCancerStatistic._id
            }

            emb2_breastCancer[i].nuclei = {
                value: emb2_breastCancer[i].nuclei,
                statistics: breastCancerStatistic._id
            }

            emb2_breastCancer[i].chromatin = {
                value: emb2_breastCancer[i].chromatin,
                statistics: breastCancerStatistic._id
            }
			
			emb2_breastCancer[i].normalNucleoli = {
                value: emb2_breastCancer[i].normalNucleoli,
                statistics: breastCancerStatistic._id
            }
			
			emb2_breastCancer[i].mitoses = {
                value: emb2_breastCancer[i].mitoses,
                statistics: breastCancerStatistic._id
            }
        };
        
        const emb2Collection = database.collection("emb2_breastCancer");
        await emb2Collection.insertMany(emb2_breastCancer);


        // 8. zadatak
        mongoDb.createIndex({"class":4,"sampleNumber":-1});
        const indexQuery = [
            {
                $match: {
                    $and: [
                            { class: { $eq: 2 } }, 
                            { sampleNumber: { $gte: breastCancerStatistic.averageStandardDeviationValues.StdDevSamp_sampleNumber } }
                        ]
                }
            }
        ];
        const indexedCollectionResult = await mongoDb.aggregate(indexQuery).toArray();
        const indexedCollection = database.collection("indexed_filtered");
        await indexedCollection.insertMany(indexedCollectionResult);

		console.log("Statistika:")
        console.log(breastCancerStatistic);
		
		console.log("Count of Supported Values:");
        console.log(suportedValuesCount);
		
        console.log("Frekvencija:")
        console.log(breastCancerFrequency);
    }
    finally {
        client.close();
    }
}


main().catch(console.log);
