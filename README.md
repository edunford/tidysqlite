# tidysqlite

A tidy data method for easy manipulation of a sqlite repository in Python.

## Motivation

 Housing data in a central repository &mdash; for either a large project or personal use &mdash; increases efficiency, consistency, and transparency. SQLite offers a light weight and easy way to create this "dataverse". However, when building and interacting with SQLite databases in Python (and [`pandas`](https://pandas.pydata.org/) in particular), one usually has to recall how to connect and draft a query.

 `tidysqlite` is a method designed to make interacting with SQLite more intuitive and simple in order to better streamline any data storage or exploration tasks. The module borrows its logic from the much beloved [`dplyr`](https://dplyr.tidyverse.org/) naming conventions and [`dbplyr`](https://dbplyr.tidyverse.org/) implementation strategy. Users build up and execute a query through a series of easily implemented (and reconizable) commands. The data can then be returned as a [`pandas`](https://pandas.pydata.org/) data frame, offering the standard [`pandas`](https://pandas.pydata.org/) toolkit to manipulate the data further.

## Installation

Download the developer version from `Github`

```
pip install --upgrade https://github.com/edunford/tidysqlite/tarball/master
```

**Note**: _The package is not yet live on PiPy._
```
# pip install tidysqlite
```

## Usage

Examples to come.

## Feedback

The motivation for `tidysqlite` was to bring some of the excellent `R`-style interfaces to Python. Both environments are fantastic tools for data exploration and analysis. All in all, the tidy-approach is central to my workflow, and I prefer not to have to make trade offs between tools when switching between languages.

That said, I've selfishly constructed the module to meet all (and only) my needs. If you have ideas on how to improve the working concept, please reach out via the issues tab or feel free to fork the repo and contribute.
