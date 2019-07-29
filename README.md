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
This backup contains 4 years of GDELT events that have been enriched with the following fields: 
title, site, summary, keywords, and meta_keys. In addition, there will be an ArcGIS Pro package to help illustrate 
key aspects of the data at our disposal. Inside the planning folder you will find a Jupyter Notebook that describes
the current quantitative findings.