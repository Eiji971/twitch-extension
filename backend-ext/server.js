const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const cookieParser = require('cookie-parser'); // Import cookie-parser

const app = express();
const port = 3001; 


app.use(cors());
app.use(cookieParser()); // Use cookie-parser middleware

// Define a middleware to set "SameSite=None; Secure" for specific cookies
const setSameSiteNoneSecure = (req, res, next) => {
    // List of cookie names to update
    const cookiesToUpdate = [
        'twitch.lohp.countryCode',
        'spare_key',
        'persistent',
        'bits_sudo',
        'name',
        'last_login',
        'api_token',
    ];

    // Loop through the list and update the cookies
    for (const cookieName of cookiesToUpdate) {
        if (req.cookies[cookieName]) {
            res.cookie(cookieName, req.cookies[cookieName], {
                sameSite: 'none',
                secure: true,
            });
        }
    }

    next(); // Continue processing the request
};

// Use the middleware to update cookies in all routes
app.use(setSameSiteNoneSecure);


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
app.get('/api/userData/:userId', (req, res) => {
    const userId = req.params.userId;
    const query = 'SELECT * FROM game.user_data WHERE user_id = ?';
    db.query(query, [userId], (err, results) => {
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


app.get('/api/playerStat/:userId', (req, res) => {
    const userId = req.params.userId;
    const query = `SELECT * FROM game.player_permanent_stat_data WHERE user_id = ?`;
  
    // Execute the SQL query
    db.query(query, [userId], (err, results) => {
        if (err) {
          console.error('Error querying the database:', err);
          res.status(500).json({ error: 'Internal server error' });
          return;
        }
    
        if (results.length === 0) {
          res.status(404).json({ error: 'User not found' });
        } else {
          const playerStat = results[0];
          res.json(playerStat);
        }
    });
});

app.get('/api/playerData/:userId', (req, res) => {
    const userId = req.params.userId;
    const query = `SELECT * FROM game.player_data WHERE user_id = ?`;
  
    // Execute the SQL query
    db.query(query, [userId], (err, results) => {
        if (err) {
          console.error('Error querying the database:', err);
          res.status(500).json({ error: 'Internal server error' });
          return;
        }
    
        if (results.length === 0) {
          res.status(404).json({ error: 'User not found' });
        } else {
          const playerData = results[0];
          res.json(playerData);
        }
    });
});

app.get('/api/itemData/:userId', (req, res) => {
    const userId = req.params.userId;
    const query = `SELECT * FROM game.player_item_data WHERE user_id = ?`;
  
    // Execute the SQL query
    db.query(query, [userId], (err, results) => {
        if (err) {
          console.error('Error querying the database:', err);
          res.status(500).json({ error: 'Internal server error' });
          return;
        }
    
        if (results.length === 0) {
          res.status(404).json({ error: 'User not found' });
        } else {
          const itemData = results[0];
          res.json(itemData);
        }
    });
});

app.get('/api/weaponData/:userId', (req, res) => {
    const userId = req.params.userId;
    const query = `SELECT * FROM game.player_weapon_data WHERE user_id = ?`;
  
    // Execute the SQL query
    db.query(query, [userId], (err, results) => {
        if (err) {
          console.error('Error querying the database:', err);
          res.status(500).json({ error: 'Internal server error' });
          return;
        }
    
        if (results.length === 0) {
          res.status(404).json({ error: 'User not found' });
        } else {
          const weaponData = results[0];
          res.json(weaponData);
        }
    });
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});