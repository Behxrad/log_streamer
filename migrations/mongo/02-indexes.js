db = db.getSiblingDB("log_db");

db.application_logs.createIndex(
    {timestamp: -1},
    {name: "timestamp_idx"}
);

db.application_logs.createIndex(
    {service_name: 1},
    {name: "service_name_idx"}
);