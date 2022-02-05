# TypeDB Offshoreleaks Database

Schema entity types
![entity types](assets/screenshot_entities.png)

## Quick start

Prerequisites: Python >3.6, [TypeDB Core](https://vaticle.com/download#core) 2.6.0, [TypeDB Python Client](https://docs.vaticle.com/docs/client-api/python) 2.6.0

### Clone this repository to your computer

```shell
git clone https://github.com/typedb-osi/typedb-data-offshoreleaks.git
```

### Set up a virtual environment and install the dependencies:


```shell
cd typedb-data-offshoreleaks
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### download and preprocess the datasets

This takes a little while!

```shell
bash ./preprocess.sh
```
### migrate the data into typedb 

```shell
# start typedb
typedb server
```
```shell
# run the migrator with 4 separate processes
python3 ./migrator.py -n 4
```

For options:
```shell
python3 migrator.py -h
```

## Licence

The data was first made available by the International Cornsortium of Investigative Journalists (ICIJ) under the [Open Database License](http://opendatacommons.org/licenses/odbl/1.0/) and the [Creative Commons Attribution-ShareAlike](http://creativecommons.org/licenses/by-sa/3.0/) license. It is re-published here under the same licences. 
The data should be used under the same terms set out on the ICIJ website: https://offshoreleaks.icij.org/pages/database.
