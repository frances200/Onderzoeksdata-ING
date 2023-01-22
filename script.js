const { Sequelize } = require('sequelize');
const fs = require("fs");
const { stringify } = require("csv-stringify");
const filename = "oracle.csv";
const writableStream = fs.createWriteStream(filename);
const Blob = require('node-blob');

const columns = [
    'inserts_amount',
    'data_bytes',
    'time_ms',
    'dbms'
];

const stringifier = stringify({ header: true, columns: columns });

const connectOracle = async () => {
    const sequelize = new Sequelize('xe', 'system', 'oracle', {
        host: 'localhost',
        port: 2660,
        dialect: 'oracle'
    });

    try {
        await sequelize.authenticate();
        console.log('Connection has been established successfully.');
        return sequelize;
    } catch (error) {
        console.error('Unable to connect to the database:', error);
    }
}

const connectPostgres = async () => {
    const sequelize = new Sequelize('test', 'postgres', 'root', {
        host: 'localhost',
        port: 5432,
        dialect: 'postgres'
    });

    try {
        await sequelize.authenticate();
        console.log('Connection has been established successfully.');
        return sequelize;
    } catch (error) {
        console.error('Unable to connect to the database:', error);
    }
}

const timeIndividualInserts = async (dbname) => {
    const database = dbname === 'oracle' ? await connectOracle() : await connectPostgres();
    const user = {
        id: '88119188-0686-4316-b7aa-fa380b687775',
        firstName: 'Barry',
        lastName: 'van Schlingeren',
        email: 'example@example.com',
        username: 'BD75OC'
    }

    const query = `INSERT INTO users VALUES ('${user.id}', '${user.firstName}', '${user.lastName}', '${user.email}', '${user.username}');`;

    for (let i = 0; i < 5000; i++) {
        await database.query('CREATE TABLE users(id VARCHAR(255), first_name VARCHAR(255) NOT NULL, last_name VARCHAR(255) NOT NULL, email VARCHAR(255) NOT NULL, username VARCHAR(255) NOT NULL, PRIMARY KEY (id));');
        const start = performance.now();
        await database.query(query)
        const end = performance.now();
        await database.query('DROP TABLE users;');
        console.log(i+1);
        stringifier.write(['1', new Blob([query]).size.toString(), `${(end - start)}`, dbname]);
    }   
}

const timeGroupInserts = async (dbname) => {
    const database = dbname === 'oracle' ? await connectOracle() : await connectPostgres();

    const query = dbname === 'oracle' ? fs.readFileSync('./users-oracle.sql', {encoding:'utf8'}).trim() : fs.readFileSync('./users-postgres.sql', {encoding:'utf8'}).trim();

    for (let i = 0; i < 1000; i++) {
        await database.query('CREATE TABLE users(id VARCHAR(255), first_name VARCHAR(255) NOT NULL, last_name VARCHAR(255) NOT NULL, email VARCHAR(255) NOT NULL, username VARCHAR(255) NOT NULL);');
        const start = performance.now();
        await database.query(query)
        const end = performance.now();
        await database.query('DROP TABLE users;')
        console.log(i+1);
        stringifier.write(['1', new Blob([query]).size.toString(), `${(end - start)}`, dbname]);
    }  
}

(async () => {
    await timeIndividualInserts('postgres');
    await timeIndividualInserts('oracle');
    await timeGroupInserts('postgres');
    await timeGroupInserts('oracle');
    stringifier.pipe(writableStream);

    // (await connectOracle()).query('DROP TABLE users;');
    // (await connectPostgres()).query('DROP TABLE users;');
})();