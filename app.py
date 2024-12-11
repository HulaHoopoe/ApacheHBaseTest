import streamlit as st
import happybase
from faker import Faker
import pandas as pd

# Настройки HBase
HBASE_HOST = 'localhost'
HBASE_PORT = 9090

# Подключение к HBase
def connect_hbase():
    return happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)

#Инициализация данных с Faker
faker = Faker()

# Создание таблиц
def initialize_tables():
    conn = connect_hbase()
    # Получаем список всех таблиц
    tables = conn.tables()

# Для каждой таблицы отключаем и удаляем
    for table in tables:
        try:
            # Отключаем таблицу
            conn.disable_table(table)
            print(f"Таблица {table} отключена.")
        
        # Удаляем таблицу
            conn.delete_table(table)
            print(f"Таблица {table} удалена.")
        except Exception as e:
            print(f"Ошибка при удалении таблицы {table}: {e}")

    # Задание схемы таблиц с уникальными именами семейств колонок
    tables = {
        "products": {
            'info': dict(),            
        },
        "categories": {
            'info': dict(),
        },
        "users": {
            'info': dict(),
        },
        "orders": {
            'info': dict(),
        },
        "order_details": {
            'info': dict(),
        },
    }

    # Проверяем, существует ли таблица, если не существует - создаем
    for table_name, families in tables.items():
        if table_name not in conn.tables():  # Если таблица не существует
            conn.create_table(table_name, families)
            st.success(f"Таблица {table_name} успешно создана!")
        else:
            st.info(f"Таблица {table_name} уже существует, пропуск создания.")

    conn.close()

# Добавление тестовых данных
def seed_data():
    conn = connect_hbase()
    products_table = conn.table('products')
    categories_table = conn.table('categories')
    users_table = conn.table('users')

        # Генерация категорий
    for i in range(1, 6):
        categories_table.put(b'category_' + str(i).encode(), {b'info:name': faker.word()})
        
        # Генерация продуктов
    for i in range(1, 101):
        products_table.put(b'product_' + str(i).encode(), {
            b'info:name': faker.word(),
            b'info:description': faker.text(),
            b'info:price': str(faker.random_number(digits=2)).encode(),
            b'info:category_id': f'category_{str(faker.random_int(1, 5))}',
        })

        # Генерация пользователей
    for i in range(1, 101):
        users_table.put(b'user_' + str(i).encode(), {b'info:name': faker.name(),
                        b'info:email': faker.email(),
                        b'info:address': faker.address(),})

    conn.close()

# Отображение таблицы
def display_table(table_name):
    conn = connect_hbase()
    table = conn.table(table_name)
    rows = []
    for key, data in table.scan():
        row = {'id': key.decode()}
        row.update({k.decode().split(':')[-1]: v.decode() for k, v in data.items()})
        rows.append(row)
    conn.close()
    if rows:
        return pd.DataFrame(rows)
    else:
        # Возвращаем пустой DataFrame с колонками 'id' и другими ожидаемыми столбцами
        return pd.DataFrame(columns=['id'])
    # return pd.DataFrame(rows)

# Добавление записи
def add_record(table_name, record_id, data):
    conn = connect_hbase()
    table = conn.table(table_name)
    table.put(record_id.encode(), {f'info:{k}'.encode(): v.encode() for k, v in data.items()})
    conn.close()

# Редактирование записи
def edit_record(table_name, record_id, data):
    conn = connect_hbase()
    table = conn.table(table_name)
    table.put(record_id.encode(), {f'info:{k}'.encode(): v.encode() for k, v in data.items()})
    conn.close()

# Удаление записи
def delete_record(table_name, record_id):
    conn = connect_hbase()
    table = conn.table(table_name)
    table.delete(record_id.encode())
    conn.close()

# Выполнение запросов к HBase
def query_hbase(query_type, category_id=None, user_id=None):
    conn = connect_hbase()
    rows = []
    match query_type:
        case "all_products":
            table = conn.table("products")
            rows = [
                {**{'id': key.decode()}, **{k.decode().split(':')[1]: v.decode() for k, v in data.items()}}
                for key, data in table.scan()
            ]
            conn.close()
            return rows

        case "all_categories":
            table = conn.table("categories")
            rows = [
                {**{'id': key.decode()}, **{k.decode().split(':')[1]: v.decode() for k, v in data.items()}}
                for key, data in table.scan()
            ]
            conn.close()
            return rows

        case "all_users":
            table = conn.table("users")
            rows = [
                {**{'id': key.decode()}, **{k.decode().split(':')[1]: v.decode() for k, v in data.items()}}
                for key, data in table.scan()
                ]
            conn.close()    
            return rows

        case "products_by_category":
            table = conn.table("products")
            rows = [
                {**{'id': key.decode()}, **{k.decode().split(':')[1]: v.decode() for k, v in data.items()}}
                for key, data in table.scan()
                if data.get(b'info:category_id', b'').decode() == category_id
            ]
            conn.close()
            return rows

        case "orders_by_user":
            table = conn.table("orders")
            rows = [
                {**{'id': key.decode()}, **{k.decode().split(':')[1]: v.decode() for k, v in data.items()}}
                for key, data in table.scan()
                if data.get(b'info:user_id', b'').decode() == user_id
            ]
            conn.close()
            return rows

        case _:
            conn.close()
            return rows

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
    # st.subheader("Данные таблицы")
    
    # st.write(df)

    # Отображение данных
    st.subheader("Данные таблицы")

    # Кнопка для обновления содержимого
    if st.button("Обновить содержимое таблицы"):
        if "dataframe" in st.session_state:
            del st.session_state["dataframe"]  # Удаляем сохранённые данные для перезагрузки

    # Загружаем таблицу только при необходимости
    if "dataframe" not in st.session_state:
        st.session_state["dataframe"] = display_table(table_name)

    # Отображаем таблицу
    st.write(st.session_state["dataframe"])
    # Список идентификаторов
    df = display_table(table_name)
    ids = df['id'].tolist()
    
    # Добавление новой записи
    st.subheader("Добавить новую запись")
    new_record_id = st.text_input("Введите ID новой записи")
    new_data = {}

    # Динамическое добавление колонок
    if "columns" not in st.session_state:
        st.session_state.columns = []  # Храним список новых колонок

    if st.checkbox("Добавить новую колонку"):
        with st.form(key="add_column_form"):
            new_col = st.text_input("Название новой колонки")
            new_val = st.text_input("Значение новой колонки")
            add_column_button = st.form_submit_button("Добавить колонку")

            if add_column_button:
                if new_col and new_val:
                    st.session_state.columns.append((new_col, new_val))
                    st.success(f"Добавлена колонка: {new_col} = {new_val}")
                else:
                    st.error("Введите название и значение колонки.")

    if st.session_state.columns:
        st.write("Текущие данные для новой записи:")
        for col, val in st.session_state.columns:
            st.write(f"- {col}: {val}")
            new_data[col] = val  # Заполняем данные

    if st.button("Сохранить запись"):
        if new_record_id and new_data:
            try:
                add_record(table_name, new_record_id, new_data)
                st.success(f"Запись {new_record_id} успешно добавлена!")
                st.session_state.columns = []  # Очищаем список колонок
            except Exception as e:
                st.error(f"Ошибка при добавлении записи: {e}")
        else:
            st.error("Заполните ID записи и добавьте хотя бы одну колонку.")

    # Редактирование записи
    st.subheader("Редактировать запись")
    selected_id = st.selectbox("Выберите ID для редактирования", ids)
    if selected_id:
        record_data = df[df['id'] == selected_id].iloc[0].to_dict()
        record_data.pop('id')  # Убираем ID из данных
        updated_data = {}

        st.write("Текущие данные записи:")
        for col, val in record_data.items():
            updated_data[col] = st.text_input(f"{col}:", val)
        
        if st.button("Сохранить изменения"):
            edit_record(table_name, selected_id, updated_data)
            st.success(f"Запись {selected_id} успешно обновлена!")

    # Удаление записи
    st.subheader("Удалить запись")
    delete_id = st.selectbox("Выберите ID для удаления", ids)
    if st.button("Удалить запись"):
        if delete_id:
            delete_record(table_name, delete_id)
            st.success(f"Запись {delete_id} успешно удалена!")
        else:
            st.error("Выберите ID для удаления.")

    # Выбор запросов
    st.subheader("Запросы")
    query_options = ["Список всех товаров", "Товары определённой категории", "Все заказы конкретного пользователя"]
    selected_query = st.selectbox("Выберите запрос для выполнения", query_options)

    if selected_query == "Список всех товаров":
        rows = query_hbase("all_products")
        if not rows:
            st.warning("Таблица 'products' пуста. Нет доступных товаров.")
        else:
            st.write("Список всех товаров:")
            st.write(pd.DataFrame(rows))

    elif selected_query == "Товары определённой категории":
        categories = query_hbase("all_categories")  # Загружаем категории из HBase
        category_ids = [row['id'] for row in categories] if categories else []
        selected_category = st.selectbox("Выберите категорию", category_ids)
        if selected_category:
            rows = query_hbase("products_by_category", category_id=selected_category)
            if not rows:
                st.warning(f"Нет товаров в категории {selected_category}.")
            else:
                st.write(f"Товары категории {selected_category}:")
                st.write(pd.DataFrame(rows))

    elif selected_query == "Все заказы конкретного пользователя":
        users = query_hbase("all_users")  # Загружаем пользователей из HBase
        user_ids = [row['id'] for row in users] if users else []
        selected_user = st.selectbox("Выберите пользователя", user_ids)
        if selected_user:
            rows = query_hbase("orders_by_user", user_id=selected_user)
            if not rows:
                st.warning(f"Нет заказов для пользователя {selected_user}.")
            else:
                st.write(f"Заказы пользователя {selected_user}:")
                st.write(pd.DataFrame(rows))
if __name__ == "__main__":
    main()
