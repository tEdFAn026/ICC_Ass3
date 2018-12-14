# University of Leicester - CO7219: Internet and Cloud Computing - Autumn 2018 - Assignment 3
**More detial:https://campus.cs.le.ac.uk/teaching/resources/CO7219/assignments/a3/**


Analysis of Data about Google Play Store Apps
This assignment counts for 40% of your coursework mark.

Format of the Input Data
In this assignment you are asked to write MapReduce programs to process Google Play Store Apps Data https://www.kaggle.com/lava18/google-play-store-apps (provided by Lavanya Gupta https://www.kaggle.com/lava18 on Kaggle https://www.kaggle.com/). There are two input files:

  * googleplaystore.csv (1.3MB, csv file, one line per App)
  * googleplaystore_user_reviews.csv (7.5MB, csv file, one line per Review)

The data files are in comma-separated values format. The file googleplaystore.csv contains the following columns:

  * App: Application name
  * Category: Category the app belongs to
  * Rating: Overall user rating of the app
  * Reviews: Number of user reviews for the app
  * Size: Size of the app
  * Installs: Number of user downloads/installs for the app
  * Type: Paid or Free (can also be NaN, which should be treated like Free)
  * Price: Price of the app
  * Content Rating: Age group the app is targeted at - Children / Mature 21+ / Adult
  * Genres: An app can belong to multiple genres (apart from its main category). For eg, a musical family game will belong to Music, Game, Family genres.
  * Last Updated: Date when the app was last updated on Play Store
  * Current Ver: Current version of the app available on Play Store
  * Android Ver: Min required Android version

The first three lines of googleplaystore.csv are:

App,Category,Rating,Reviews,Size,Installs,Type,Price,Content Rating,Genres,Last Updated,Current Ver,Android Ver
Photo Editor & Candy Camera & Grid & ScrapBook,ART_AND_DESIGN,4.1,159,19M,"10,000+",Free,0,Everyone,Art & Design,"January 7, 2018",1.0.0,4.0.3 and up
Coloring book moana,ART_AND_DESIGN,3.9,967,14M,"500,000+",Free,0,Everyone,Art & Design;Pretend Play,"January 15, 2018",2.0.0,4.0.3 and up

The file googleplaystore_user_reviews.csv contains the following columns:

  * App: Name of app
  * Translated_Review: User review (Preprocessed and translated to English)
  * Sentiment: Positive/Negative/Neutral (Preprocessed)
  * Sentiment_Polarity: Sentiment polarity score
  * Sentiment_Subjectivity: Sentiment subjectivity score
  
The first three lines of googleplaystore_user_reviews.csv are:

App,Translated_Review,Sentiment,Sentiment_Polarity,Sentiment_Subjectivity
10 Best Foods for You,"I like eat delicious food. That's I'm cooking food myself, case ""10 Best Foods"" helps lot, also ""Best Before (Shelf Life)""",Positive,1.0,0.5333333333333333
10 Best Foods for You,This help eating healthy exercise regular basis,Positive,0.25,0.28846153846153844

Each of the files starts with a header line (the column headers indicate the meaning of the columns). If a field contains a comma inside it, the value of the field is enclosed in quotation marks. If a field contains commas and quotation marks, each quotation mark is escaped by putting another quotation mark in front of it. (This is the standard for comma-separated files.)
Tasks

Your task is to solve the following two problems by writing MapReduce programs.

1 (Task 1, 50%) For this task you will only use the file googleplaystore.csv as input. For every app category (second column of the input file), calculate the following values:
  * Number of Free apps in that category
  * Number of Paid apps in that category
  * Average price of Paid apps in that category
  
Each key-value pair in the output should have as key the name of the category (for example, "ART_AND_DESIGN"), and as value a string giving the number of Free apps, followed by a comma and a space, followed by the number of Paid apps, followed by a comma and a space, followed by the average price of the Paid apps (as decimal number with two decimal places (i.e. rounded to two decimal places)). 
If the input file contains duplicate rows (i.e., different rows for the same App), it is okay if that App is counted multiple times (i.e., you do not need to write code to eliminate duplicates). 
Implement your solution in a class Category, and call the source file Category.java. 

2 (Task 2, 50%) For this task you will use both files googleplaystore.csv (referred to as file A) and googleplaystore_user_reviews.csv (referred to as file B) as input. Find all Apps that satisfy the following conditions:
  * The App has a Content Rating of "Everyone" (in file A).
  * The App has at least 50 reviews in file B for which the Sentiment_polarity is not 'nan'. We call such reviews valid.
  * The average Sentiment_polarity of the valid reviews of the App in file B is greater than or equal to 0.3.
  
For every App that satisfies all these conditions, the output should contain a key-value pair that has as key the name of the App, and as value a String containing the following pieces of information (with a comma followed by a space as separator between consecutive values):

  * The Category of the App (from file A).
  * The number of valid reviews of the App in file B.
  * The average Sentiment_polarity of the valid reviews of the App in file B (as decimal number with two decimal places (i.e. rounded to two decimal places)).
  
Implement your solution in a class Sentiment, and call the source file Sentiment.java.

For full marks, you should use a combiner (or even the 'in-mapper combining' design pattern) to reduce the number of key-value pairs that need to be sent from mappers to reducers. Comment your code appropriately. The comments at the top of each java file should briefly explain how you have solved the task and describe any noteworthy features or optimisations that you have implemented within the code.

Your programs should be written in such a way that they also work with any number of input files of arbitrary size (i.e., you should not assume that one Mapper will process the whole file A and the other Mapper will process the whole file B), and with any number of mappers and reducers that the Hadoop runtime chooses to create.
