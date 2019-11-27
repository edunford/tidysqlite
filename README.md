# tidysqlite

A tidy data method for easily manipulating a sqlite database in Python.

## Motivation

As a data researcher, it makes a lot of sense to store and local data in a SQLite data base. Housing data in a central repository for a project or personal use increases efficiency, consistency, and transparency of any data project one might be working on. However, Python (and [`pandas`](https://pandas.pydata.org/) in particular) has no streamlined way of interfacing with a SQLite database. `tidysqlite` is a response to that deficiency. The module borrows its logic from the much beloved [`dplyr`](https://dplyr.tidyverse.org/) naming conventions and [`dbplyr`](https://dbplyr.tidyverse.org/) implementation strategy. Users build up and execute a query through a series of easily implemented (and reconizable) commands. Rendered data is returned as a [`pandas`](https://pandas.pydata.org/) data frame. One can easily use their familiar [`pandas`](https://pandas.pydata.org/) toolkit to manipulate the data further.

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
