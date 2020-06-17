// const nano = require('nano')('http://admin:admin@localhost:5984');
const MongoClient = require('mongodb').MongoClient;
const mysql = require('mysql2/promise');
const _ = require('lodash');

const limit = 100;
const client = new MongoClient('mongodb://localhost:27017');

async function doWork() {
  const sqlDb = await mysql.createPool({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'imdb2',
    connectionLimit: 50,
  });

  await client.connect();
  const mongoDb = client.db('imdb');
  const collection = mongoDb.collection('name_basics');

  //clear database
  // await nano.db.destroy('imdb2');
  // await nano.db.create('imdb2');
  // const couchDb = nano.use('imdb2');

  let time = Date.now();

  console.log('querying count...');
  const [maxRows] = await sqlDb.query(
    `select max(nconst) as max_nconst from name_basics`
  );
  const maxNconst = maxRows[0]['max_nconst'];
  console.log('finished count', maxNconst, '-', Date.now() - time);
  const offsets = [];

  for (let offset = 0; offset < maxNconst; offset += limit) {
    offsets.push(offset);
  }

  console.log(`offsets ${offsets.length}`);

  for (const offset of offsets) {
    time = Date.now();
    console.log('querying name_basics offset', offset);
    const [rowsBasics] = await sqlDb.query(
      `select * from name_basics where nconst > ${offset} and nconst < ${
        offset + limit
      }`
    );
    console.log('finished querying name_basics offset', '-', Date.now() - time);

    time = Date.now();
    console.log('querying title_principals offset', offset);
    const [rowsPrincipals] = await sqlDb.query(
      `select * from title_principals where nconst > ${offset} and nconst < ${
        offset + limit
      }`
    );
    console.log(
      'finished querying title_principals offset',
      '-',
      Date.now() - time
    );

    const principalsMap = [];

    time = Date.now();
    console.log('normalizing offset', offset);
    rowsPrincipals.forEach(r => {
      r.characters = r.characters ? r.characters.split(',') : null;
      r.titleId = `t${r.tconst}`;

      let principals = principalsMap[r.nconst];

      if (!principals) {
        principalsMap[r.nconst] = principals = [];
      }

      principals.push(rowsPrincipals);
      delete r.nconst;
    });

    rowsBasics.forEach(r => {
      r._id = `n${r.nconst}`;
      r.knownForTitles = r.knownForTitles ? r.knownForTitles.split(',') : null;

      r.principals = principalsMap[r.nconst];

      delete r.nconst;
      delete r.sn_soundex;
      delete r.ns_soundex;
      delete r.s_soundex;
    });
    console.log('finizhed normalizing offset', '-', Date.now() - time);

    time = Date.now();
    console.log('inserting into couchdb...');

    for (const chunk of _.chunk(rowsBasics, 100)) {
      collection.insertMany(chunk);
      // await couchDb.bulk({
      //   docs: chunk,
      // });
    }

    console.log('finished inserting into mongodb', '-', Date.now() - time);
    // console.log('finished inserting into couchdb', '-', Date.now() - time);
  }

  // console.log('finished sql...');
  // console.log('parsing data...');
  // rows.forEach(r => {
  //   r._id = `t${r.tconst}`;
  //   r.genres = r.genres ? r.genres.split(',') : null;
  //   r.isAdult = !!r.isAdult;
  //   delete r.tconst;
  // });
  // console.log('end parsing data...')
  // console.log('inserting into couchdb...')
  // const promises = [];
  // _.chunk(rows, 50000).forEach(values => {
  //   promises.push(couchDb.bulk({
  //     docs: values
  //   }));
  // });
  // await Promise.all(promises);
  // // await couchDb.bulk({
  // //   docs: rows,
  // // });
  // console.log('finished insert...')
  process.exit(0);
}

doWork();
