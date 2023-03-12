# Eclaims to FHIR Transformer
Developed by Health TAG ([@Lukespacewalker](https://github.com/Lukespacewalker)) and H Lab ([@methdevth](https://github.com/methdevth))

*This readme is authored by [@Lukespacewalker](https://github.com/Lukespacewalker) and GitHub Co-pilot.*

## Dependencies
1. Python 3.11
2. Pipenv

## Installation
1. Clone the repository
2. Install the dependencies
```bash
pipenv install
```
3. Enter the virtualenv
```bash
pipenv shell
```

## Usages (After entering the virtualenv)
### Watch the directory for changes

The program will watch for any new folder containing the Eclaims files (TXT and DBF) in `watchingdir`.  For example, place Eclaims files in `watchingdir/txt/elciams_20230312/*.txt`. 
The program will then transform the files and output the result in the `watchingdir/txt/elciams_20230312/output` directory.
It will also create `watchingdir/txt/elciams_20230312/done` file to prevent the script from repeatedly transforming the same folder over and over again.

#### How to run the program
If your prompt is in `fhir-eclaim-transformer`, you can now run the following command to watch the directory for changes.
```bash
python . --watch
```
Or if you are in the parent folder of `fhir-eclaim-transformer`, you can run the following command
```bash
python fhir-eclaim-transformer --watch
```
The output will be like this
```bash
***********************************
* FHIR 17 documents Transformer v1*
*           9 March 2023          *
***********************************
Checking on workingdir folder for any existing work
-> workingdir
-> workingdir\dbf
-> workingdir\txt
-> workingdir\txt\test
workingdir\txt\test will be processed as E-Claims Folders.
Processing eclaims folder at C:\Users\Sutti\source\repos\fhir-eclaim-transformer\workingdir\txt\test
Creating Patient Resource: 100%|████████████████████████████████████| 9521/9521 [00:05<00:00, 1813.33it/s]
Creating Other Resources: 100%|█████████████████████████████████████| 14272/14272 [01:10<00:00, 202.02it/s]
Saving result to workingdir\txt\test\output\bundle_1.json
Successfully save result to workingdir\txt\test\output\bundle_1.json
Create Bundle Json Successfully
Saving result to workingdir\txt\test\output\bundle_2.json
Successfully save result to workingdir\txt\test\output\bundle_2.json
```

### Transform a single folder
If you want to transform a single folder, you can run the following command
```bash
python . --path <folder path>
```

## Configuration
The configuration file is located at `.env`.  You can change the following settings
- `VALIDATE_FHIR_RESOURCES` - Set to `1` to validate the FHIR resources.  Set to `0` to skip the validation. 
Default is `1`. If you skip the validation, the transforming process will be much faster at the risk of invalid resource 
according to FHIR Specification. These invalid resources may be rejected by FHIR Server.