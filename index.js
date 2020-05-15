const nano = require('nano')('http://localhost:5984');
const mysql = require('mysql2/promise');

const couchDb = nano.use('imdb');

async function doWork() {
  const sqlDb = await mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'imdb2',
    connectionLimit: 50,
  });
  const [rows, fields] = await sqlDb.query(`select * from title_basics`);
  await couchDb.bulk({
    docs: fields,
  });
}

doWork();
