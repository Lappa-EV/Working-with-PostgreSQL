-- Создание таблиц 
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT NOW(),
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);


-- 1. Функция логирования изменений
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
DECLARE
    v_changed_by TEXT := CURRENT_USER;
    field_name TEXT;
    old_value TEXT;
    new_value TEXT;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        FOR field_name, old_value, new_value IN
            SELECT 'name', OLD.name::TEXT, NEW.name::TEXT
            UNION ALL
            SELECT 'email', OLD.email::TEXT, NEW.email::TEXT
            UNION ALL
            SELECT 'role', OLD.role::TEXT, NEW.role::TEXT
        LOOP
            IF old_value IS DISTINCT FROM new_value THEN
                INSERT INTO users_audit (user_id, changed_at, changed_by, field_changed, old_value, new_value)
                VALUES (OLD.id, NOW(), v_changed_by, field_name, old_value, new_value);
            END IF;
        END LOOP;

        RETURN NEW;
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql;


-- 2. Триггер на таблицу users
CREATE TRIGGER users_audit_trigger
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();


-- 3. Установка расширения pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;


-- 4. Функция для экспорта изменений за сегодняшний день
CREATE OR REPLACE FUNCTION export_daily_user_audit()
RETURNS VOID AS $$
DECLARE
    v_file_path TEXT := '/tmp/users_audit_export_' || to_char(CURRENT_DATE, 'YYYYMMDD') || '.csv';
BEGIN
    -- Экспортируем данные
    EXECUTE format('COPY (SELECT * FROM users_audit WHERE changed_at::date = CURRENT_DATE) TO %L WITH CSV HEADER', v_file_path);

    -- Логирование
    RAISE NOTICE 'Exported daily user audit to %', v_file_path;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error during export: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;


-- 5. Установка планировщика pg_cron на 3:00 ночи
SELECT cron.schedule('0 3 * * *', 'SELECT export_daily_user_audit()');


-- Проверка планировщика
SELECT * FROM cron.job;

-- Заполняем таблицу
INSERT INTO users (name, email, role) VALUES
('John Doe', 'john.doe@example.com', 'user'),
('Jane Smith', 'jane.smith@example.com', 'admin');

UPDATE users SET name = 'John Updated', role = 'administrator' WHERE id = 1;
UPDATE users SET email = 'jane.new@example.com' WHERE id = 2;
UPDATE users SET email = 'jane.new@example.co' WHERE id = 2;

-- Проверка наполняемости таблиц
SELECT * FROM users;
SELECT * FROM users_audit;

-- Вызов функции для экспорта изменений
SELECT export_daily_user_audit();


