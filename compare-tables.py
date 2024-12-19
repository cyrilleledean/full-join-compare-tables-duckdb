def create_faker_db(n: int):

    import duckdb

    with duckdb.connect("./fake-data.duckdb") as con:

        con.sql("""
            CREATE OR REPLACE TABLE fake_a (
                name VARCHAR, email VARCHAR, address VARCHAR, phone VARCHAR
            )
        """)

        from faker import Faker
        import pyarrow as pa

        fake = Faker('fr_FR')

        data = []

        print(f"Génération de {n} lignes de données ...")

        for _ in range(n):

            data.append({
                "name": fake.name(),"email": fake.email(),
                "address": fake.address(),"phone": fake.phone_number()
            })

        print(f"Chargement dans table fake_a ...")

        # Chargement du jeu de test dans la table fake_a
        table = pa.Table.from_pylist(data)
        con.register("temp_table", table)
        con.execute("INSERT INTO fake_a SELECT * FROM temp_table")

        print(f"Copie de la table fake_a vers la table fake_b...")

        # Copie de fake_a dans une table fake_b
        con.sql("CREATE OR REPLACE TABLE fake_b AS SELECT * FROM fake_a")

        # Ajout de lignes pour simuler des modifications à detecter

        con.sql("""
            INSERT into fake_a values 
                ('Laura', 'laura@example.net', 'GB', '00 00 00 00 00')
            """
        )
        con.sql("""
            INSERT into fake_b values 
                ('Laura', 'laura@example.net', 'GB', '00 00 00 00 01');
            """
        )        
        con.sql("""
            INSERT into fake_b values 
                ('Benedicte', 'benedicte@example.net', 'FR', '00 00 00 00 00');
            """
        )

        SQL_COMPARE = """
            WITH
                source_a AS (
                    SELECT 
                        'fake_a' AS origine, 
                        fake_a AS colonnes, 
                        sha256(fake_a::text) AS hash 
                    FROM 
                        fake_a
                ),
                source_b AS (
                    SELECT 
                        'fake_b' AS origine, 
                        fake_b AS colonnes, 
                        sha256(fake_b::text) AS hash 
                    FROM 
                        fake_b
            )
            SELECT 
                COALESCE(source_a.origine, source_b.origine) AS origine,
                UNNEST(COALESCE(source_a.colonnes, source_b.colonnes))
            FROM 
                source_a
            FULL JOIN 
                source_b 
            ON 
                source_a.hash = source_b.hash
            WHERE
                source_a.hash IS NULL OR source_b.hash IS NULL
            ORDER BY 2, 1
        """

        print(con.sql(SQL_COMPARE).show())


if __name__ == "__main__":

    create_faker_db(1000000)