# Wikipedia Article Pageviews
This repository automatically fetches and aggregates the 100 most popular Wikipedia articles by pageviews - creating a dataset that enables tracking trending topics on Wikipedia.

It works by polling the WikiMedia API on a daily basis and fetching the top 100 most popular articles from two days ago. 
The fetcher runs in a scheduled GitHub Actions workflow, which is [available here](https://github.com/vtasca/wikipedia-pageviews/actions/workflows/main.yml).

The dataset begins in the year 2016 and the textual data is presented as it is found on the website of Wikipedia.

## Usage
The updated dataset is located in this repository at [`pageviews.csv`](https://github.com/vtasca/wikipedia-pageviews/blob/master/pageviews.csv). Data for day $D_{t-2}$ is added on day $D_t$ at noon.

### Data description
- `rank` - Rank of the article (out of 100).
- `article` - Title of the article.
- `views` - Number of pageviews (across all platforms).
- `date` - Date of the pageviews.

### Availability
The dataset is also available on [Kaggle](https://www.kaggle.com/datasets/vladtasca/wikipedia-pageviews), together with related Jupyter notebooks.

## Related research
Wikipedia content has been analyzed in various ways in academic research, for instance to summarize topics in an unsupervised manner[^1] or to predict voting behavior in elections[^2].
[^1]: Ahn, B. G., Van Durme, B., & Callison-Burch, C. (2011, June). WikiTopics: What is popular on Wikipedia and why. In Proceedings of the workshop on automatic summarization for different genres, media, and languages (pp. 33-40).
[^2]: Smith, B. K., & Gustafson, A. (2017). Using wikipedia to predict election outcomes: online behavior as a predictor of voting. Public Opinion Quarterly, 81(3), 714-735.

## Background information
[Wikipedia](https://www.wikipedia.org) is a free, multilingual online encyclopedia created and maintained through open collaboration by a global community of volunteer contributors. Launched in 2001, it has grown to become one of the world's most visited websites, containing millions of articles in hundreds of languages on virtually every topic imaginable. Its unique model allows anyone to edit most articles, while relying on a complex system of community oversight, editorial policies, and citation requirements to maintain accuracy and combat vandalism.
