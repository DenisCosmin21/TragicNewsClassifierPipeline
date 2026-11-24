package org.rubigdata.Core

case class DataBaseConfig(
    url: String,
    user: String,
    password: String,
    driver: String,
    main_table: String,
    staging_table: String
);