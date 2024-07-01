const express = require('express');
const path = require('path');
const app = express();
const port = 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.engine('ejs', require('ejs').__express);

app.set('view engine', 'ejs');

app.get('/', (req, res) => {
  res.render('parquetFiles', {
    title: 'Parquet Files',
    port: port,
  });
});

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});
