# ercot-project

## Setting up Git LFS

To track .csv files with Git LFS, run the following commands:

```sh
git lfs install
git lfs track "*.csv"
```

Remember to commit the changes to the .gitattributes file:

```sh
git add .gitattributes
git commit -m "Track .csv files with Git LFS"
```

## Setting up the SQLite Database

To set up the SQLite database with the required tables, run the following script:

```sh
python src/setup-database.py
```