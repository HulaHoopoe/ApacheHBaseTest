import streamlit as st
import happybase
from faker import Faker
import pandas as pd

# Настройки HBase
HBASE_HOST = 'localhost'
HBASE_PORT = 9090

# Функция для подключения к HBase
def connect_hbase():
    return happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)

# Инициализация данных с Faker
faker = Faker()

# Создание таблиц
def initialize_tables():
    conn = connect_hbase()
    tables = {
        "products": ["info:name", "info:description", "info:price", "info:category_id"],
        "categories": ["info:name"],
        "users": ["info:name", "info:email", "info:address"],
        "orders": ["info:user_id", "info:status"],
        "order_details": ["info:order_id", "info:product_id", "info:quantity", "info:price"],
    }
    for table_name, families in tables.items():
        if table_name.encode() not in conn.tables():
            conn.create_table(table_name, {family: dict() for family in families})
    conn.close()

# Добавление тестовых данных
def seed_data():
    conn = connect_hbase()
    products_table = conn.table('products')
    categories_table = conn.table('categories')
    users_table = conn.table('users')
    
    # Генерация категорий
    for i in range(1, 6):
        categories_table.put(f'category_{i}', {b'info:name': faker.word()})
    
    # Генерация продуктов
    for i in range(1, 101):
        products_table.put(f'product_{i}', {
            b'info:name': faker.word(),
            b'info:description': faker.text(),
            b'info:price': str(faker.random_number(digits=2)),
            b'info:category_id': f'category_{faker.random_int(1, 5)}'.encode(),
        })
    
    # Генерация пользователей
    for i in range(1, 101):
        users_table.put(f'user_{i}', {
            b'info:name': faker.name(),
            b'info:email': faker.email(),
            b'info:address': faker.address(),
        })
    conn.close()

# Отображение таблицы
def display_table(table_name):
    conn = connect_hbase()
    table = conn.table(table_name)
    rows = []
    for key, data in table.scan():
        row = {'id': key.decode()}
        row.update({k.decode(): v.decode() for k, v in data.items()})
        rows.append(row)
    conn.close()
    return pd.DataFrame(rows)

# CRUD: добавление записи
def add_record(table_name, data):
    conn = connect_hbase()
    table = conn.table(table_name)
    key = f"{table_name}_{faker.random_int(1000, 9999)}"
    table.put(key, {k.encode(): v.encode() for k, v in data.items()})
    conn.close()

# CRUD: редактирование записи
def edit_record(table_name, record_id, data):
    conn = connect_hbase()
    table = conn.table(table_name)
    table.put(record_id, {k.encode(): v.encode() for k, v in data.items()})
    conn.close()

# CRUD: удаление записи
def delete_record(table_name, record_id):
    conn = connect_hbase()
    table = conn.table(table_name)
    table.delete(record_id)
    conn.close()

# Интерфейс Streamlit
def main():
    st.title("Apache HBase CRUD с Streamlit")
    
    # Инициализация базы данных
    if st.button("Инициализировать базу данных"):
        initialize_tables()
        seed_data()
        st.success("Таблицы созданы и наполнены данными!")
    
    # Выбор таблицы
    table_name = st.selectbox("Выберите таблицу", ["products", "categories", "users", "orders", "order_details"])
    
    # Отображение данных
    if st.button("Показать данные"):
        df = display_table(table_name)
        st.write(df)
    
    # Добавление новой записи
    if st.checkbox("Добавить новую запись"):
        with st.form(key="add_form"):
            fields = st.text_area("Введите данные в формате JSON (например, {\"info:name\": \"Example\"})")
            submit_button = st.form_submit_button(label="Добавить")
            if submit_button:
                try:
                    data = eval(fields)
                    add_record(table_name, data)
                    st.success("Запись успешно добавлена!")
                except Exception as e:
                    st.error(f"Ошибка: {e}")
    
    # Редактирование записи
    if st.checkbox("Редактировать запись"):
        df = display_table(table_name)
        record_id = st.selectbox("Выберите ID записи", df['id'].tolist())
        with st.form(key="edit_form"):
            record_data = st.text_area("Введите новые данные в формате JSON (например, {\"info:name\": \"Updated\"})")
            submit_button = st.form_submit_button(label="Сохранить изменения")
            if submit_button:
                try:
                    data = eval(record_data)
                    edit_record(table_name, record_id, data)
                    st.success("Запись успешно обновлена!")
                except Exception as e:
                    st.error(f"Ошибка: {e}")
    
    # Удаление записи
    if st.checkbox("Удалить запись"):
        df = display_table(table_name)
        record_id = st.selectbox("Выберите ID записи для удаления", df['id'].tolist())
        if st.button("Удалить"):
            try:
                delete_record(table_name, record_id)
                st.success("Запись успешно удалена!")
            except Exception as e:
                st.error(f"Ошибка: {e}")

if __name__ == "__main__":
    main()