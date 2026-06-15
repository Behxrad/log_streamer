db = db.getSiblingDB("log_db");

db.createUser({
    user: "app_user",
    pwd: "app_password",
    roles: [
        {
            role: "readWrite",
            db: "log_db"
        }
    ]
});

db.createCollection("application_logs");