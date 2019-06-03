# Visualizing Hate with GDELT

This repository contains the beginnings of a simple toolset that enriches daily GDELT 1.0 
data. Using NLP tools like newspaper3k to add keywords and summaries to each GDELT event will allow us to 
apply more filters to the data and ask more focused questions. Our initial goal is to explore
how the term "hate" has manifested in the media over the past few years. While everything in 
this repository is looking back in time, adjusting this code base to routinely check daily events
and integrate/corroborate with other media outlets would be a trivial task.   

# Getting Started

You can pull down a backup of the most recent PostgreSQL 
database from [Google Drive](https://drive.google.com/drive/folders/1bKCVqyX23mUCjZUpB6J4yQl49QFddTiS?usp=sharing). 
 This backup contains 3 years of GDELT events that have been enriched with the following fields: 
 title, site, summary, keywords, and meta_keys. In addition, there will be an ArcGIS Pro package to help illustrate key aspects of the data at our disposal. If you 
are not interested in accessing the raw data, there is a simple 
[Esri Web AppBuilder](https://dbsne.maps.arcgis.com/apps/webappviewer/index.html?id=3773616afbc2400ba149fc0b2805b6b4) application that will be used
to help understand what we are currently doing with this research. 