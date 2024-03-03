| *Step* | *Operation*                                    | *Location*                                                               |
|--------|------------------------------------------------|--------------------------------------------------------------------------|
| 0.1.   | Modelling strategy                             | Local machine                                                            |
| 0.2.   | Preparation/clean-up                           | Local machine                                                            |
| 1.     | Verify schema, create job manifest (incl UUID) | Local machine: existing Python program OR web app served by RAP frontend |
| 2.     | Upload job to Google cloud bucket              | Local machine: albeit backing onto GCP storage                           |
| 3.     | Append job metadata SQLite database            | RAP service: monitoring GCP API and fetching new data                    |
| 4.     | One or more job runs                           | RAP service: according to manifest                                       |
| 5.     | Cache result cached in SQLite database         | RAP service                                                              |
| 6.     | Create canonical URL of result and/or data-set | RAP service                                                              |
