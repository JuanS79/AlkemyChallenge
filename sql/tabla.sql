CREATE TABLE IF NOT EXISTS tabla_prueba(
            ext_date date PRIMARY KEY,
            id_provincia Integer NOT NULL,
            id_departamento Integer NOT NULL,
            categoria VARCHAR(255) NOT NULL,
            provincia VARCHAR(255) NOT NULL,
            localidad VARCHAR(255) NOT NULL,
            nombre VARCHAR (255) NOT NULL,
            domicilio VARCHAR (255) NOT NULL,
            codigo_postal VARCHAR (255) NOT NULL,
            número_teléfono VARCHAR (255) NOT NULL,
            mail VARCHAR(255) NOT NULL,
            web VARCHAR (255) NOT NULL
            );