# IMDB Crawler

For a detailed report, please see: 

## Installation

To crawl data and reproduce the analysis, the following packages must be installed:

- numpy
- pandas
- scipy
- scikit-learn
- statsmodels
- beautifulsoup4
- matplotlib
- streamlit
- seaborn
- altair
- SQLAlchemy
- tqdm
- sqlalchemy_schemadisplay

These packages can be installed by running:

```bash
pip install -r requirements.txt
```

The code is written with Python 3.8. As such, please use the same version of Python to guarantee reproducibility. Use `venv` or `conda` to create a new environment for isolation.

## To-do

- [X] (25%) Crawl top 250 movies data
- [X] (30%) Crawl top 250 movies rating data
- [X] (40%) Crawl director/actor/writer pages
- [ ] ~~(45%) Crawl top 1000 popular movies data~~
- [ ] (45%) Implement logging/testing/check for code coverage
- [X] (55%) Implement sqlite database
- [X] (60%) Documentation for crawler
- [X] (85%) Analysis (notebook)
- [ ] (100%) Report
- [ ] (110%) App (streamlit?)