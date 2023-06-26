using CSV
using DotEnv
using LibPQ
using DataFrames
DotEnv.load()

function make_dataframe()
    return vcat([ CSV.File("data/$(filename)") |> DataFrame for filename in readdir("data/")]...)
end

println("Iniciando conexão com o banco de dados")
connection = LibPQ.Connection("dbname=$(ENV["DB_NAME"]) user=$(ENV["USER"]) password=$(ENV["PASSWORD"])")
println("Conexão feita com sucesso")

println("Carregando datasets para memoria . . .")
dataframe = make_dataframe()
println("Dataset carregado para memoria")
rename!(dataframe, [Symbol(lowercase(string(col))) for col in names(dataframe)])


#=
println("Enviando DataFrame para a tabela 'srag' no banco de dados...")
query = """CREATE TABLE IF NOT EXISTS public.povoamento_srag(  
);"""
LibPQ.execute(connection, query)
LibPQ.load!(connection, "srag", dataframe; if_exists="append", chunk_size=100000)
println("DataFrame enviado para a tabela 'srag' no banco de dados")

# Fechando a conexão com o banco de dados
LibPQ.finish(connection)
=#