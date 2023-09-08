const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');

const app = express();
const port = 3001; 


app.use(cors());

// Create a MySQL database connection
const db = mysql.createConnection({
    host: '127.0.0.1',
    user: 'root',
    password: 'Orobou',
    database: 'game',
});  

// Connect to the database
db.connect((err) => {
  if (err) {
    console.error('Error connecting to the database:', err);
    return;
  }
  console.log('Connected to the database');
});

// Define an API endpoint to fetch user data
app.get('/api/userData', (req, res) => {
  const userId = 26583153; // Replace with the actual user ID
  const query = `SELECT * FROM game.user_data WHERE userID = ${userId}`;

  // Execute the SQL query
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error querying the database:', err);
      res.status(500).json({ error: 'Internal server error' });
      return;
    }

    if (results.length === 0) {
      res.status(404).json({ error: 'User not found' });
    } else {
      const userData = results[0];
      res.json(userData);
    }
  });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});