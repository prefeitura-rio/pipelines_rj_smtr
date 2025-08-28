import json
import os

from google.cloud import bigquery


def extrair_descricoes_de_modelos(manifest):
    tabelas = []

    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model":
            schema = node.get("schema")
            table_name = node.get("alias") or node.get("name")
            descricao = node.get("description", "").strip()
            column_descriptions = {}

            for col_name, col_data in node.get("columns", {}).items():
                desc = col_data.get("description", "").strip()
                if desc:
                    column_descriptions[col_name] = desc

            tabelas.append(
                {
                    "schema": schema,
                    "table_name": table_name,
                    "description": descricao,
                    "column_descriptions": column_descriptions,
                }
            )

    return tabelas


def atualizar_descricao_tabela(client, projeto, schema, nome, descricao_tabela, descricoes_colunas):
    table_id = f"{projeto}.{schema}.{nome}"
    table_def = client.get_table(table_id)
    tabela = table_def.to_api_repr()

    if descricao_tabela and tabela["description"] != descricao_tabela:
        if len(descricao_tabela) > 16384:
            print(
                f"A descrição da da tabela '{table_id}' tem mais de 16384 caracteres, \
                    não é possível atualizar."
            )
        else:
            print(f"Atualizando a descrição da tabela '{table_id}'")
            tabela["description"] = descricao_tabela

    for i in range(len(tabela["schema"]["fields"])):
        field = tabela["schema"]["fields"][i]
        if field["name"] in descricoes_colunas and descricoes_colunas[field["name"]]:
            if (
                "description" in field and field["description"] != descricoes_colunas[field["name"]]
            ) or "description" not in field:
                if len(descricoes_colunas[field["name"]]) > 1024:
                    print(
                        f"A descrição da coluna '{field['name']}' da tabela '{table_id}' \
                            tem mais de 1024 caracteres, não é possível atualizar."
                    )
                    continue
                print(f"Atualizando a descrição da coluna '{field['name']}' da tabela '{table_id}'")
                tabela["schema"]["fields"][i]["description"] = descricoes_colunas[field["name"]]

    tabela = table_def.from_api_repr(tabela)
    client.update_table(tabela, ["description", "schema"])


def main():
    credentials = json.loads(os.getenv("BQ_AUTH_SA"))
    client = bigquery.Client.from_service_account_info(credentials, project="rj-smtr")
    with open("queries/target/manifest.json", "r") as f:
        manifest = json.load(f)
    modelos = extrair_descricoes_de_modelos(manifest)

    for modelo in modelos:
        try:
            atualizar_descricao_tabela(
                client,
                "rj-smtr",
                modelo["schema"],
                modelo["table_name"],
                modelo["description"],
                modelo["column_descriptions"],
            )
        except Exception as e:
            print(f"Error: {modelo['table_name']}: {e}")


if __name__ == "__main__":
    main()
