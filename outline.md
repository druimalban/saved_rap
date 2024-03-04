| *Step* | *Operation*                                        | *Location*                                                                                                             |
|--------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| 0.1    | Modelling strategy                                 | Local machine                                                                                                          |
| 0.2    | Preparation/clean-up                               | Local machine                                                                                                          |
| 1      | Verify schema, create job manifest (incl UUID)     | Local machine: existing Python program OR web app served by RAP frontend (dependent on level of control over versions) |
| 2      | Upload job data-set to Google cloud bucket         | Local machine: albeit backs onto GCP storage                                                                           |
| 3      | Append job metadata to queue and SQLite database   | RAP service: monitor GCP API for changes                                                                               |
| 4      | Fetch data required by job(s)                      | RAP service: pull via GCP API                                                                                          |
| 5      | Run job(s)                                         | RAP service                                                                                                            |
| 6      | Cache result(s) in SQLite database and/or data-set | RAP service                                                                                                            |
| 7      | Create canonical URL of result and/or data-set     | RAP service                                                                                                            |
