// var http = require('http');

// //create a server object:
// http.createServer(function (req, res) {
//   res.write('Hello World!'); //write a response to the client
//   res.end(); //end the response
// }).listen(8080); //the server object listens on port 8080

const express = require("express");
const { MongoClient } = require("mongodb");
const path = require("path"); // Import the 'path' module
const http = require("http"); // Import the 'http' module
const { DateTime } = require("luxon");



// function formatTimeInCountry(timestamp, country) {
//   try {
//     const timezone = moment.tz.guess(); // Get user's timezone
//     const formattedTime = moment(timestamp).tz(timezone).format("HH:mm"); // Convert and format time

//     return formattedTime;
//   } catch (error) {
//     console.error("Error formatting time:", error);
//     return timestamp; // Return unformatted time in case of error
//   }
// }



const app = express();
const port = process.env.PORT || 3000;


// Set the views folder path relative to the src directory
app.set("views", path.join(__dirname, "views"));

// Set up your middleware
app.set("view engine", "ejs");
app.use(express.static(path.join(__dirname, "assets")));
// app.use(express.static(path.join(__dirname, "public"))); // Use 'path.join' to create the correct static path

// console.log(express.static(path.join(__dirname, "/public")));

const mongoUrl = "mongodb://localhost:27017/";
const dbName = "flight_db";
const collectionName = "flight_details";


app.get("/", async (req, res) => {
  let client;  // Define the client variable outside the try block

  try {
    client = new MongoClient(mongoUrl);
    await client.connect();

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const data = await collection.find({ $and: [
          { departed_from_iata: {$exists : true, $ne : ""} },
          { dest_iata: { $exists : true, $ne: "" } },
          { event_lat: { $exists : true, $ne: "" } },
          { event_lon: { $exists : true, $ne: "" } },
          { flight: { $exists : true } },
        ] 
    }).toArray();

    res.render("index", { data });
  } catch (error) {
    console.error("Error fetching data from MongoDB:", error);
    res.render("index", { data: [] });
  } finally {
    if (client) {
      client.close();
    }
  }
});



const server = http.createServer(app);

server.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
