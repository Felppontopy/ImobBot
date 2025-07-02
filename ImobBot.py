import os
import re
import time
import threading
import unicodedata
import pandas as pd
import logging
import asyncio
from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ConversationHandler, ContextTypes
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from bs4.element import Tag
import openai
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('imobbot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Carrega variáveis do .env
load_dotenv()
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN') or os.getenv('TELEGRAM_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai.api_key = OPENAI_API_KEY

# --- Controle global de estado ---
active_scraping_tasks = {}  # {user_id: {'cancelled': bool, 'thread': threading.Thread}}
scraping_lock = threading.Lock()

# --- Constantes e dados ---
ZONAS_RJ = {
    "Zona Central": ["Centro", "Catumbi", "Cidade Nova", "Estácio", "Gamboa", "Lapa", "Mangueira", "Paquetá", "Rio Comprido", "Santa Teresa", "Santo Cristo", "Saúde", "Vasco da Gama"],
    "Zona Sul": ["Botafogo", "Glória", "Catete", "Copacabana", "Cosme Velho", "Flamengo", "Gávea", "Humaitá", "Ipanema", "Jardim Botânico", "Lagoa", "Laranjeiras", "Leblon", "Leme", "Rocinha", "São Conrado", "Urca", "Vidigal"],
    "Zona Oeste": ["Anil", "Barra da Tijuca", "Barra de Guaratiba", "Camorim", "Cidade de Deus", "Curicica", "Freguesia (Jacarepaguá)", "Gardênia Azul", "Grumari", "Itanhangá", "Jacarepaguá", "Joá", "Praça Seca", "Pechincha", "Recreio dos Bandeirantes", "Tanque", "Taquara", "Vargem Grande", "Vargem Pequena", "Vila Valqueire", "Bangu", "Deodoro", "Gericinó", "Jardim Sulacap", "Magalhães Bastos", "Padre Miguel", "Realengo", "Santíssimo", "Senador Camará", "Vila Militar", "Campo Grande", "Cosmos", "Guaratiba", "Inhoaíba", "Paciência", "Pedra de Guaratiba", "Santa Cruz", "Senador Vasconcelos", "Sepetiba"],
    "Zona Norte": ["Acari", "Pavuna ", "São Cristóvão", "Benfica", "Alto da Boa Vista", "Andaraí", "Abolição", "Água Santa", "Cachambi", "Caju", "Del Castilho", "Encantado", "Engenho de Dentro", "Engenho Novo", "Grajaú", "Higienópolis", "Jacaré", "Jacarezinho", "Lins de Vasconcelos", "Manguinhos", "Maria da Graça", "Maracanã", "Méier", "Piedade", "Pilares", "Praça da Bandeira", "Riachuelo", "Rocha", "Sampaio", "São Francisco Xavier", "Tijuca", "Vila Isabel", "Bancários", "Bonsucesso", "Cacuia", "Cocotá", "Freguesia (Ilha do Governador)", "Galeão", "Jardim Carioca", "Jardim Guanabara", "Maré", "Moneró", "Olaria", "Pitangueiras", "Portuguesa", "Praia da Bandeira", "Ramos", "Ribeira", "Tauá", "Zumbi", "Acari", "Anchieta", "Barros Filho", "Bento Ribeiro", "Brás de Pina", "Campinho", "Cavalcanti", "Cascadura", "Coelho Neto", "Colégio", "Cordovil", "Costa Barros", "Engenheiro Leal", "Engenho da Rainha", "Guadalupe", "Honório Gurgel", "Inhaúma", "Irajá", "Jardim América", "Madureira", "Marechal Hermes", "Oswaldo Cruz", "Parada de Lucas", "Parque Anchieta", "Parque Colúmbia", "Pavuna", "Penha", "Penha Circular", "Quintino Bocaiuva", "Ricardo de Albuquerque", "Rocha Miranda", "Tomás Coelho", "Turiaçu", "Vaz Lobo", "Vicente de Carvalho", "Vigário Geral", "Vila da Penha", "Vila Kosmos", "Vista Alegre"]
}

# Carrega cidades validadas do interior do RJ (arquivo deve estar no mesmo diretório)
try:
    with open("cidades_rj_validadas.txt", "r", encoding="utf-8") as f:
        CIDADES_RJ = [line.strip() for line in f if line.strip()]
    logger.info(f"✅ Loaded {len(CIDADES_RJ)} cities from cidades_rj_validadas.txt")
    if len(CIDADES_RJ) == 0:
        logger.warning("⚠️ File cidades_rj_validadas.txt is empty")
except FileNotFoundError:
    logger.error("❌ File cidades_rj_validadas.txt not found")
    CIDADES_RJ = []
except Exception as e:
    logger.error(f"❌ Error loading cidades_rj_validadas.txt: {str(e)}")
    CIDADES_RJ = []

# Dados das cidades do interior do RJ com seus bairros
CIDADES_INTERIOR_BAIRROS = {
    "Angra dos Reis": [
        "Abraão (Ilha Grande)", "Areal", "Balneário", "Belém", "Bonfim", "Camorim",
        "Camorim Pequeno", "Centro", "Enseada (Ilha Grande)", "Enseada das Estrelas (Ilha Grande)",
        "Frade", "Gamboa do Bracuí", "Garatucaia", "Jacuecanga", "Japuíba", "Marinas",
        "Mombaça", "Monsuaba", "Morro da Carioca", "Morro da Cruz", "Morro do Carmo",
        "Morro do Peres", "Parque das Palmeiras", "Perequê", "Praia Brava", "Praia do Anil",
        "Praia Grande", "Retiro", "Ribeira", "Santa Rita do Bracuí", "Santo Antônio",
        "São Bento", "Sapé", "Sertão do Bracuí", "Vila do Abraão", "Vila Histórica de Mambucaba",
        "Vila Velha"
    ],
    "Aperibé": [
        "Centro", "Ponte Seca", "Vila Tostes", "Presidente Kenedy", "Nossa Senhora de Fátima",
        "São João", "Verdes Vales"
    ],
    "Araruama": [
        "Areal", "Bananeiras", "Barbudo", "Boa Perna", "Centro", "Coqueiral",
        "Engenho Grande", "Fazendinha", "Hawai", "Hospício", "Iguabinha", "Japão",
        "Jardim Califórnia", "Jardim São Paulo", "Monteiro", "Morro Grande",
        "Nossa Senhora de Nazareth", "Novo Horizonte", "Outeiro", "Paracatu",
        "Parque Hotel", "Parati", "Pernambuca", "Ponta do Capim", "Pontinha",
        "Praia do Hospício", "Praia Seca", "São Vicente de Paulo", "Viaduto", "Vila Capri"
    ],
    "Areal": [
        "Centro", "Distrito de Alberto Torres", "Granja São José", "Vila de Areal"
    ],
    "Armação dos Búzios": [
        "Alto de Búzios", "Armação", "Baía Formosa", "Barra da Lagoa", "Brava", "Canto",
        "Centro", "Ferradura", "Forno", "Geribá", "João Fernandes", "José Gonçalves",
        "Manguinhos", "Ossos", "Praia Rasa", "São José", "Tartaruga", "Tucuns"
    ],
    "Arraial do Cabo": [
        "Caiçara", "Caminho do Pontal", "Canaa", "Centro", "Figueira", "Macedônia",
        "Monte Alto", "Morro da Boa Vista", "Morro da Cabocla", "Parque das Garças",
        "Pernambuca", "Pontal", "Prainha", "Praia dos Anjos", "Praia Grande", "Sítio",
        "Taio", "Vila Canaã", "Vila Industrial"
    ],
    "Barra do Piraí": [
        "Asa Branca", "Caixa D'água Velha", "Califórnia da Barra", "Carvão", "Centro",
        "Chácara Farani", "Coimbra", "Coqueiros", "Distrito de Ipiabas", "Dr. Mesquita",
        "Grota do Neném", "Horto", "Maracanã", "Matadouro", "Morro do Gama", "Muqueca",
        "Oficinas Velhas", "Parque Santana", "Roseira", "Santo Antônio", "São João",
        "São Luís", "Vargem Grande", "Vila Helena", "Vila Suíça"
    ],
    "Barra Mansa": [
        "Ano Bom", "Apiadeiro", "Boa Sorte", "Boa Vista", "Bocaininha", "Centro",
        "Colônia Santo Antônio", "Estamparia", "Jardim América", "Jardim Boa Vista",
        "Jardim Central", "Jardim Marilu", "Jardim Primavera", "Light",
        "Nossa Senhora do Amparo", "Nova Esperança", "Piteiras", "Rialto", "Roselândia",
        "Santa Clara", "Santa Izabel", "Santa Rosa", "São Francisco de Assis",
        "São Judas Tadeu", "São Luiz", "São Silvestre", "Saudade", "Siderlândia",
        "Verbo Divino", "Vila Coringa", "Vila Elmira", "Vila Independência", "Vila Maria",
        "Vila Nova", "Vila Orlandélia", "Vila Principal", "Vila Ursulino", "Vista Alegre"
    ],
    "Belford Roxo": [
        "Areia Branca", "Barro Vermelho", "Bayer", "Bom Pastor", "Centro", "Coelho da Rocha",
        "Farrula", "Heliópolis", "Itaipu", "Jardim do Ipê", "Jardim Gláucia", "Lote XV",
        "Nova Aurora", "Piam", "Redentor", "Santa Amélia", "Santa Maria", "Santa Teresa",
        "Santo Antônio da Prata", "São Bernardo", "São Francisco de Assis", "São Vicente",
        "Sargento Roncalli", "Vale do Ipê", "Vila Pauline", "Xavantes"
    ],
    "Bom Jardim": [
        "Alto de Santa Cruz", "Bairro de Fátima", "Centro", "Jardim Ornellas", "Maravilha",
        "Parque das Águas", "São Miguel", "Varginha", "Vila da Amizade"
    ],
    "Bom Jesus do Itabapoana": [
        "Centro", "Bela Vista", "Pimentel Marques", "Lia Márcia", "Novo", "Monte Calvário",
        "José Lima", "Parque do Trevo"
    ],
    "Cabo Frio": [
        "Algodoal", "Braga", "Caminho de Búzios", "Centro", "Dunas do Peró", "Foguete",
        "Gamboa", "Guarani", "Itajuru", "Jardim Caiçara", "Jardim Excelsior",
        "Jardim Flamboyant", "Jardim Olinda", "Jardim Peró", "Ogiva", "Palmeiras",
        "Parque Burle", "Passagem", "Peró", "Portinho", "Praia do Siqueira",
        "Recanto das Dunas", "São Bento", "São Cristóvão", "Unamar", "Vila do Sol", "Vila Nova"
    ],
     "Cachoeiras de Macacu": [
        "Centro", "Japuíba", "Papucaia", "Subaio", "Boca do Mato", "Funchal",
        "Castália", "Valério"
    ],
    "Cambuci": [
        "Centro", "Floresta", "Cruzeiro", "São João do Paraíso", "Três Irmãos", "Funil",
        "Monte Verde"
    ],
    "Campos dos Goytacazes": [
        "Centro", "Parque Califórnia", "Parque Tamandaré", "Parque Santo Amaro",
        "Parque Rosário", "Pelinca", "Parque Leopoldina", "Horto Municipal",
        "Jardim Carioca", "Parque Turf Club", "Parque Corrientes", "Parque São Caetano",
        "Parque Tarcísio Miranda", "Goytacazes", "Donana", "Goitacazes", "Farol de São Tomé",
        "Travessão", "Guarus", "Ururaí", "Dores de Macabu", "Mundo Novo", "Tocos",
        "Santo Eduardo", "Santa Maria"
    ],
    "Cantagalo": [
        "Centro", "São José", "Parque das Árvores", "Triângulo", "Santo Antônio",
        "São Pedro", "Boa Sorte"
    ],
    "Carapebus": [
        "Centro", "Sapecado", "Urbis", "Praia de Carapebus", "Capelinha", "Vila Cândida"
    ],
    "Cardoso Moreira": [
        "Centro", "Cachoeiro", "Orminda", "Catarino", "Parque das Acácias",
        "Nossa Senhora da Penha"
    ],
    "Carmo": [
        "Centro", "Botafogo", "Herdeiros", "Jardim Carmo", "Nossa Senhora da Glória",
        "Parque Industrial"
    ],
    "Casimiro de Abreu": [
        "Centro", "Barra de São João", "Rio Dourado", "Professor Souza", "Mataruna",
        "Industrial", "Jardim Miramar"
    ],
    "Comendador Levy Gasparian": [
        "Centro", "Afonso Arinos", "Fonseca Almeida", "Grotão", "Gulf"
    ],
    "Conceição de Macabu": [
        "Centro", "Bocaina", "Vila São José", "Rhódia", "Piteiras", "Calçadinha"
    ],
    "Cordeiro": [
        "Centro", "Retiro Poético", "Dois Valos", "Manancial", "Rodolfo", "São Luiz"
    ],
    "Duas Barras": [
        "Centro", "Jardim do Lago", "Matadouro", "Vargem Grande", "Fazenda do Campo"
    ],
    "Duque de Caxias": [
        "Bar dos Cavalheiros", "Centro", "Centenário", "Chácaras Arcampo", "Doutor Laureano",
        "Engenho do Porto", "Gramacho", "Jardim Gramacho", "Jardim Leal", "Jardim Olavo Bilac",
        "Lagunas e Dourados", "Parque Duque", "Parque Fluminense", "Parque Sarapuí",
        "Periquitos", "Sarapuí", "Vila São Luís", "Vila Sarapuí", "Vinte e Cinco de Agosto",
        "Campos Elíseos", "Capivari", "Chácaras Rio-Petrópolis", "Cidade dos Meninos",
        "Figueira", "Imbariê", "Jardim Anhangá", "Jardim Primavera", "Mardi Gras",
        "Nova Campinas", "Pilar", "Parada Angélica", "Parada Morabi", "Parque Eldorado",
        "Parque Equitativa", "Santa Cruz da Serra", "Santa Lúcia", "Saracuruna", "Taquara",
        "Vila Maria Helena", "Vila Santa Cruz", "Xerém"
    ],
    "Engenheiro Paulo de Frontin": [
        "Centro", "Rodolfo de Abreu", "Graminha", "Sacra Família do Tinguá", "Morro Azul"
    ],
    "Guapimirim": [
        "Bananal", "Centro", "Citrolândia", "Corujas", "Gleba de Fora", "Iconha",
        "Jardim Guapimirim", "Limoeiro", "Monte Olivete", "Orindi", "Parada Ideal",
        "Parque das Rosas", "Quinta Mariana", "Segredo", "Vale das Pedrinhas", "Vila Olímpia"
    ],
    "Iguaba Grande": [
        "Centro", "Canellas City", "Cidade Nova", "Iguabela", "Jardim Solares",
        "Lagoa Azul", "Parque Tamariz", "Pedreira"
    ],
    "Itaboraí": [
        "Aldeia da Prata", "Ampliação", "Apolo", "Caluge", "Centro", "Chácaras de Inoã",
        "Gebara", "Granjas Cabuçu", "Itambi", "Joaquim de Oliveira", "Manilha",
        "Marambaia", "Nancilândia", "Nova Cidade", "Outeiro das Pedras", "Pachecos",
        "Porto das Caixas", "Retiro São Joaquim", "Rio Várzea", "Sambaetiba",
        "Santo Expedito", "São Joaquim", "São José", "Três Pontes", "Venda das Pedras",
        "Visconde de Itaboraí"
    ],
    "Itaguaí": [
        "Centro", "Vila Margarida", "Engenho", "Parque Brisamar", "Monte Serrat",
        "Jardim América", "Leandro", "Coroa Grande", "Ilha da Madeira", "Vila Geni",
        "Chaperó", "Ibirapitanga", "Mazomba", "Piranema"
    ],
    "Italva": [
        "Centro", "Boa Vista", "Parque Industrial", "São Caetano", "Saldanha da Gama"
    ],
    "Itaocara": [
        "Centro", "Cidade Nova", "Jardim da Aldeia", "Bocaina", "Caxias", "Adolvane"
    ],
    "Itaperuna": [
        "Aeroporto", "Bela Vista", "Boa Fortuna", "Carulas", "Centro", "Cidade Nova",
        "Cehab", "Fiteiro", "Frigorífico", "Glória", "Governo", "Horto Florestal",
        "Jardim Surubi", "Lions", "Matadouro", "Niterói", "Presidente Costa e Silva",
        "Presidente Humberto de Alencar Castelo Branco", "São Francisco", "São José",
        "São Mateus", "Vale do Sol", "Vinhosa"
    ],
    "Itatiaia": [
        "Centro", "Jardim Itatiaia", "Vila Magnólia", "Vila Odete", "Maromba", "Maringá", "Penedo"
    ],
    "Japeri": [
        "Alecrim", "Belo Horizonte", "Beira-Mar", "Centro", "Chacrinha", "Cidade Jardim",
        "Engenheiro Pedreira", "Eucaliptos", "Guandu", "Jardim Delamare", "Jardim Primavera",
        "Jardim Semeador", "Lagoa do Sapo", "Laranjal", "Marajoara", "Mucajá", "Nova Belém",
        "Parque Mucajá", "Santa Amélia", "Santa Inês", "São Jorge", "Vila Central",
        "Vila Conceição", "Virgem de Fátima"
    ],
    "Macaé": [
        "Aeroporto", "Aroeira", "Barra de Macaé", "Botafogo", "Cajueiros", "Campo do Oeste",
        "Cancela Preta", "Cavaleiros", "Centro", "Costa do Sol", "Engenho da Praia",
        "Eixo Sul", "Glória", "Granja dos Cavaleiros", "Horto", "Imbetiba", "Imboassica",
        "Jardim Guanabara", "Jardim Santo Antônio", "Jardim Vitória", "Lagoa", "Lagomar",
        "Miramar", "Mirante da Lagoa", "Nova Brasília", "Nova Holanda", "Novo Cavaleiros",
        "Parque Aeroporto", "Parque de Tubos", "Parque Valentina Miranda", "Pecado",
        "Praia Campista", "Riviera Fluminense", "Santa Mônica", "Sol y Mar", "Vale Encantado",
        "Virgem Santa", "Visconde de Araújo"
    ],
    "Macuco": [
        "Centro", "Barreira", "Glória", "Reta"
    ],
    "Magé": [
        "Centro (Magé)", "Flexeiras", "Barbuda", "Pico", "Vila Nova", "Saco",
        "Centro (Vila Inhomirim)", "Fragoso", "Piabetá", "Suruí", "Guia de Pacobaíba",
        "Mauá", "Pau Grande", "Rio do Ouro", "Santo Aleixo"
    ],
    "Mangaratiba": [
        "Centro", "Conceição de Jacareí", "Ibicuí", "Itacuruçá", "Muriqui",
        "Praia do Saco", "Serra do Piloto", "Vila de Muriqui"
    ],
    "Maricá": [
        "Araçatiba", "Barra de Maricá", "Caju", "Caxito", "Centro", "Condado de Maricá",
        "Flamengo", "Guaratiba", "Inoã", "Itaipuaçu", "Jacaroá", "Jaconé",
        "Jardim Atlântico", "Mumbuca", "Parque Nanci", "Pindobas", "Ponta Grossa",
        "Ponta Negra", "Recanto de Itaipuaçu", "São José do Imbassaí", "Spar",
        "Ubatiba", "Zacarias"
    ],
    "Mendes": [
        "Centro", "Independência", "Humberto Antunes", "Santa Rita", "Vila Mariana"
    ],
    "Mesquita": [
        "Alto Uruguai", "Banco de Areia", "Centro", "Chatuba", "Coreia", "Cosmorama",
        "Edson Passos", "Jacutinga", "Juscelino", "Rocha Sobrinho", "Santa Terezinha",
        "Santo Elias", "Vila Emil"
    ],
    "Miguel Pereira": [
        "Arcádia", "Barão de Javary", "Centro", "Conrado", "Governador Portela",
        "Lagoa das Lontras", "Paes Leme", "Plante Café", "Ramada", "Vera Cruz", "Vila Suíça"
    ],
    "Miracema": [
        "Centro", "Santa Terezinha", "Cehab", "Vila Nova", "Santa Cruz", "Rodolfo"
    ],
    "Natividade": [
        "Centro", "Sindicato", "Liberdade", "Popular", "Balança"
    ],
    "Nilópolis": [
        "Cabuís", "Centro", "Nova Cidade", "Manoel Reis", "Nossa Senhora de Fátima",
        "Novo Horizonte", "Olinda", "Paiol de Pólvora", "Santos Dumont", "Tropical"
    ],
    "Niterói": [
        "Badu", "Baldeador", "Barreto", "Boa Viagem", "Cachoeiras", "Cafubá",
        "Camboinhas", "Cantagalo", "Caramujo", "Centro", "Charitas", "Cubango",
        "Engenhoca", "Engenho do Mato", "Fátima", "Fonseca", "Gragoatá", "Icaraí",
        "Ilha da Conceição", "Ingá", "Itaipu", "Ititioca", "Jacaré", "Jurujuba",
        "Largo da Batalha", "Maceió", "Maria Paula", "Matapaca", "Morro do Estado",
        "Muriqui", "Pé Pequeno", "Piratininga", "Ponta d'Areia", "Rio do Ouro",
        "Santa Bárbara", "Santa Rosa", "Santana", "São Domingos", "São Francisco",
        "São Lourenço", "Sapê", "Tenente Jardim", "Várzea das Moças", "Viçoso Jardim",
        "Viradouro", "Vital Brazil"
    ],
    "Nova Friburgo": [
        "Amparo", "Braunes", "Caledônia", "Cardinot", "Cascatinha", "Centro",
        "Chácara do Paraíso", "Conselheiro Paulino", "Cônego", "Duas Pedras",
        "Jardim Califórnia", "Jardim Ouro Preto", "Lagoinha", "Lumiar", "Mury",
        "Olaria", "Paissandu", "Parque São Clemente", "Ponte da Saudade", "Prado",
        "Riograndina", "São Geraldo", "São Pedro da Serra", "Suspiro", "Vale dos Pinheiros",
        "Varginha", "Vila Guarani", "Vilage"
    ],
    "Nova Iguaçu": [
        "Adrianópolis", "Austin", "Bairro da Luz", "Boa Esperança", "Califórnia",
        "Caonze", "Centro", "Cobrex", "Comendador Soares", "Figueiras", "Jardim Alvorada",
        "Jardim Guandu", "Jardim Iguaçu", "Jardim Nova Era", "Jardim Palmares",
        "Jardim Tropical", "Kennedy", "Km 32", "Miguel Couto", "Moquetá", "Nova América",
        "Palhada", "Parque Ambaí", "Posse", "Prata", "Rancho Novo", "Riachão",
        "Santa Eugênia", "Tinguá", "Vila de Cava", "Vila Nova", "Vila Operária"
    ],
    "Paracambi": [
        "Centro", "Cascata", "Fábrica", "Guarajuba", "Jardim Nova Era", "Lages",
        "Parque Industrial", "Sabugo", "São José", "Vila São José"
    ],
    "Paraíba do Sul": [
        "Centro", "Jatobá", "Liberdade", "Ponte", "Salutaris", "Vila Salutaris"
    ],
    "Paraty": [
        "Barra Grande", "Caboclo", "Caborê", "Centro Histórico", "Chácara da Saudade",
        "Corumbê", "Cunha", "Fátima", "Jabaquara", "Laranjeiras", "Mangueira",
        "Paraty Mirim", "Patrimônio", "Ponte Branca", "Pontal", "Portão de Ferro",
        "Praia Grande", "São Gonçalo", "Saudade", "Sertão do Taquari", "Trindade"
    ],
    "Paty do Alferes": [
        "Centro", "Arcozelo", "Avelar", "Monte Alegre", "Palmares"
    ],
    "Petrópolis": [
        "Alto da Serra", "Araras", "Bairro Castrioto", "Bingen", "Cascatinha",
        "Castelânea", "Centro Histórico", "Chácara Flora", "Cidale", "Coronel Veiga",
        "Corrêas", "Duarte da Silveira", "Duchas", "Fazenda Inglesa", "Floresta",
        "Independência", "Itaipava", "Itamarati", "Jardim Americano", "Mosela",
        "Nogueira", "Pedro do Rio", "Posse", "Quitandinha", "Retiro", "Samambaia",
        "São Sebastião", "Secretário", "Siméria", "Valparaíso", "Vila Militar"
    ],
    "Pinheiral": [
        "Centro", "Cruzeiro", "Parque Maíra", "Rolamão", "Varjão"
    ],
    "Piraí": [
        "Centro", "Santanésia", "Arrozal", "Jaqueira", "Varjão"
    ],
    "Porciúncula": [
        "Centro", "João Clóvis", "Ilha", "Operário", "Santo Antônio"
    ],
    "Porto Real": [
        "Centro", "Freitas Soares", "Jardim das Acácias", "Nova Colônia", "Parque Mariana"
    ],
    "Quatis": [
        "Centro", "Barrinha", "Jardim Independência", "Nossa Senhora do Rosário", "Pilotos"
    ],
    "Queimados": [
        "Aliança", "Belmonte", "Centro", "Coimbra", "Fanchem", "Glória", "Inconfidência",
        "Jardim da Fonte", "Jardim do Trevo", "Jardim São Miguel", "Nossa Senhora da Conceição",
        "Paraíso", "Parque Ipanema", "Parque Valdariosa", "Ponte Preta", "São Francisco",
        "São Roque", "Tri-Campeão", "Vila do Tinguá", "Vila Nascente", "Vila Pacaembu",
        "Vila Americana", "Vila Guimarães"
    ],
    "Quissamã": [
        "Centro", "Barra do Furado", "Canto da Saudade", "Machadinha", "Piteiras"
    ],
    "Resende": [
        "Alto dos Passos", "Barbosa Lima", "Baixada da Olaria", "Cabral", "Campos Elíseos",
        "Centro", "Cidade Alegria", "Engenheiro Passos", "Fazenda da Barra", "Ipiranga",
        "Itapuca", "Jardim Aliança", "Jardim Beira Rio", "Jardim Brasília", "Jardim Jalisco",
        "Liberdade", "Manejo", "Mirante das Agulhas", "Montese", "Morada da Colina",
        "Morada do Contorno", "Morada da Barra", "Nova Liberdade", "Paraíso",
        "Parque Ipiranga", "Parque Zito", "Penhasco", "Penedo", "Serrinha do Alambari",
        "Surubi", "Toyota", "Vicentina", "Vila Julieta", "Vila Moderna", "Vila Santa Isabel"
    ],
    "Rio Bonito": [
        "Centro", "Boqueirão", "Praça Cruzeiro", "Rio do Ouro", "Jacuba", "Mangueirinha"
    ],
    "Rio Claro": [
        "Centro", "Lídice", "Passa Três", "Getulândia", "São João Marcos"
    ],
    "Rio das Flores": [
        "Centro", "Abarracamento", "Cachoeira do Funil", "Manuel Duarte", "Taboas"
    ],
    "Rio das Ostras": [
        "Alphaville", "Ancora", "Balneário Remanso", "Boca da Barra", "Centro",
        "Chácara Mariléa", "Cidade Beira Mar", "Cidade Praiana", "Colinas", "Costazul",
        "Enseada das Gaivotas", "Extensão do Bosque", "Floresta das Gaivotas",
        "Jardim Bela Vista", "Jardim Campomar", "Jardim Mariléa", "Jardim Miramar",
        "Liberdade", "Mar do Norte", "Nova Cidade", "Novo Rio das Ostras", "Operário",
        "Palmital", "Parque da Cidade", "Parque Zabulão", "Praia Mar", "Recanto",
        "Reduto da Paz", "Recreio", "Rocha Leão", "Santa Irene", "São Cristóvão",
        "Terra Firme", "Verde Mar", "Village Rio das Ostras", "Zen"
    ],
        "Santa Maria Madalena": [
        "Centro", "Arrastão", "Itapuá", "Parque Itaporanga", "Santo Antônio do Imbé"
    ],
    "Santo Antônio de Pádua": [
        "Centro", "Cidade Nova", "Dezesseis", "Ferreira", "Gerador", "São Félix"
    ],
    "São Fidélis": [
        "Centro", "Barão de Macaúbas", "Coroados", "Ipuca", "Montese", "Penha"
    ],
    "São Francisco de Itabapoana": [
        "Centro", "Barra de Itabapoana", "Gargaú", "Guaxindiba", "Santa Clara", "Sonhos"
    ],
    "São Gonçalo": [
        "Alcântara", "Antonina", "Boaçu", "Brasilândia", "Centro", "Colubandê", "Coelho",
        "Cruzeiro do Sul", "Engenho do Roçado", "Engenho Pequeno", "Estrela do Norte",
        "Fazenda dos Mineiros", "Galo Branco", "Gradim", "Guaxindiba", "Itaoca", "Itaúna",
        "Jardim Amendoeira", "Jardim Catarina", "Jardim Imperial", "Jóquei", "Laranjal",
        "Lindo Parque", "Luiz Caçador", "Maria Paula", "Mutondo", "Mutuá", "Neves",
        "Nova Cidade", "Pacheco", "Paraíso", "Parada 40", "Patronato", "Pita",
        "Porto da Madama", "Porto da Pedra", "Porto do Rosa", "Porto Novo", "Porto Velho",
        "Raul Veiga", "Recanto das Acácias", "Rocha", "Rosane", "Salgueiro", "Santa Catarina",
        "Santa Izabel", "Santa Luzia", "São Miguel", "Sete Pontes", "Tenente Jardim",
        "Tribobó", "Trindade", "Vila Lage", "Vila Três", "Vista Alegre", "Zé Garoto"
    ],
    "São João da Barra": [
        "Centro", "Atafona", "Grussaí", "Cajueiro", "Chapéu de Sol", "Barcelos"
    ],
    "São João de Meriti": [
        "Agostinho Porto", "Centro", "Coelho da Rocha", "Éden", "Engenheiro Belford",
        "Gato Preto", "Grande Rio", "Jardim Meriti", "Jardim Metrópole", "Jardim Sumaré",
        "Parque Alian", "Parque Analândia", "Parque Araruama", "Parque Tietê", "São Mateus",
        "Tomazinho", "Venda Velha", "Vila Norma", "Vila Rosali", "Vilar dos Teles"
    ],
    "São José de Ubá": [
        "Centro", "Betel", "Divinéia", "João Valim"
    ],
    "São José do Vale do Rio Preto": [
        "Centro", "Águas Claras", "Jaguara", "Parada Morelli", "Rio Bonito"
    ],
    "São Pedro da Aldeia": [
        "Centro", "Balneário", "Baixo Grande", "Boqueirão", "Flexeira", "Nova São Pedro",
        "Poço Fundo", "Praia Linda"
    ],
    "São Sebastião do Alto": [
        "Centro", "Ipituna", "Valão do Barro", "Ibipeba"
    ],
    "Sapucaia": [
        "Centro", "Aparecida", "Anta", "Jamapará", "Pião"
    ],
    "Saquarema": [
        "Água Branca", "Bacaxá", "Barra Nova", "Boqueirão", "Centro", "Coqueiral",
        "Gravatá", "Ipitangas", "Itaúna", "Jaconé", "Jardim", "Leigo", "Mombaça",
        "Porto da Roça", "Retiro", "Sampaio Corrêa", "Vilatur"
    ],
    "Seropédica": [
        "Boa Esperança", "Campo Lindo", "Centro", "Fazenda Caxias", "Incra",
        "Jardim Maracanã", "Piranema", "Santa Sofia", "São Miguel"
    ],
    "Silva Jardim": [
        "Centro", "Boqueirão", "Caxias", "Cidade Nova", "Imbaú"
    ],
    "Sumidouro": [
        "Centro", "Campinas", "Dona Mariana", "Soledade", "Vila de Cima"
    ],
    "Tanguá": [
        "Centro", "Ampliação", "Bandeirantes", "Duques", "Vila Cortes"
    ],
    "Teresópolis": [
        "Agriões", "Albuquerque", "Alto", "Araras", "Barra do Imbuí", "Bom Retiro",
        "Caxangá", "Comary", "Corta Vento", "Ermitage", "Fátima", "Fonte Santa",
        "Granja Comary", "Granja Guarani", "Iúcas", "Jardim Cascata", "Jardim Meudon",
        "Meudon", "Painera", "Paineiras", "Panorama", "Parque do Imbuí", "Parque São Luiz",
        "Pimenteiras", "Posse", "Prata", "Quebra Frascos", "Quinta Lebrão", "Santa Cecília",
        "São Pedro", "Soberbo", "Tijuca", "Várzea"
    ],
    "Trajano de Moraes": [
        "Centro", "Visconde de Imbé", "Sodrelândia", "Ponte de Zinco"
    ],
    "Três Rios": [
        "Bemposta", "Cantagalo", "Centro", "Cidade Nova", "Hermitage", "Jaqueline",
        "Monte Castelo", "Nova Niterói", "Pilões", "Portão", "Purys", "Santa Teresinha",
        "Triângulo", "Vila Isabel", "Werner Silveira"
    ],
    "Valença": [
        "Alicácio", "Bairro de Fátima", "Belo Horizonte", "Benedito", "Centro",
        "Chacrinha", "Conservatória", "Esteves", "Hildebrando Lopes", "Jardim Valença",
        "João Bonito", "Osório", "Paraíso", "Parque Pentagna", "Santa Cruz",
        "Santa Isabel do Rio Preto", "São Francisco", "Serra da Glória", "Spalla", "Varginha"
    ],
    "Vassouras": [
        "Andrade Costa", "Centro", "Demétrio Ribeiro", "Ferroviários", "Guaíba", "Greco",
        "Ipiranga", "Madruga", "Massambará", "Matadouro", "Mendes", "Residência",

        "Rovisco Pais", "Santa Amália", "Sebastião Lacerda", "Vila dos Ferroviários"
    ],
    "Volta Redonda": [
        "Açude", "Aero Clube", "Água Limpa", "Aterrado", "Bairro do Retiro", "Barreira Cravo",
        "Belo Horizonte", "Belmonte", "Brasilândia", "Caieiras", "Candelária", "Casa de Pedra",
        "Cinquentenário", "Coqueiros", "Conforto", "Dom Bosco", "Eucaliptal", "Jardim Amália",
        "Jardim Belvedere", "Jardim Normândia", "Jardim Paraíba", "Jardim Ponte Alta",
        "Laranjal", "Limoeiro", "Monte Castelo", "Morada da Colina", "Niterói",
        "Nova Primavera", "Nova São Luiz", "Padre Josimo", "Ponte Alta", "Retiro", "Roma",
        "Sampaio", "Santa Cruz", "Santa Inês", "Santa Rita do Zarur", "Santo Agostinho",
        "São Cristóvão", "São Geraldo", "São João Batista", "São Lucas", "Sessenta",
        "Siderlândia", "Siderópolis", "Três Poços", "Vila Americana", "Vila Brasília",
        "Vila Mury", "Vila Rica", "Vila Santa Cecília", "Voldac"
    ]

}

TIPOS_IMOVEL = [
    "Apartamento", 
    "Casa", 
    "Casa de Condomínio",
    "Cobertura Residencial",
    "Kitnet Residencial",
    "Flat Residencial",
    "Terreno",
    "Prédio Residencial",
    "Prédio Comercial",
    "Sala Comercial",
    "Galpões Comerciais", 
    "Pontos Comerciais",
    "Consultórios Comerciais",
    "Imóveis Comerciais",
    "Fazendas / Sitios"
]
MODALIDADES = ["Aluguel", "Venda"]

# --- Estados da conversa ---
(ESCOLHA_LOCAL, ESCOLHA_ZONA, ESCOLHA_BAIRRO, ESCOLHA_CIDADE, ESCOLHA_ZONA_COMPLETA, ESCOLHA_CIDADE_INTERIOR, ESCOLHA_BAIRRO_INTERIOR, ESCOLHA_TIPO, ESCOLHA_MODALIDADE, ESCOLHA_REFINAMENTO, ESCOLHA_PAGINAS, CONFIRMA_BUSCA, AGUARDA_SCRAPING) = range(13)

# --- Função utilitária para GPT-4o mini ---
def gpt4o_ask(prompt, system=None):
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    
    logger.info(f"🤖 OpenAI Request - Prompt: {prompt[:100]}...")
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            max_tokens=300,
            temperature=0.2,
        )
        content = response.choices[0].message.content
        result = content.strip() if content else ""
        logger.info(f"🤖 OpenAI Response: {result[:100]}...")
        return result
    except Exception as e:
        logger.error(f"❌ OpenAI Error: {str(e)}")
        return "Desculpe, houve um erro na comunicação. Tente novamente."

# --- Funções utilitárias ---
def normalize_str(s):
    if not s:
        return ""
    # Remove acentos e caracteres especiais
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    # Converte para minúsculas e substitui espaços por hífens
    s = s.lower().strip()
    s = re.sub(r'[^\w\s-]', '', s) # Remove caracteres não alfanuméricos, exceto espaços e hífens
    s = re.sub(r'[-\s]+', '-', s) # Substitui um ou mais espaços/hífens por um único hífen
    return s.strip('-')

def build_vivareal_url(context):
    """
    Monta a URL do Viva Real para busca, seguindo o padrão exato do site
    """
    base_url = "https://www.vivareal.com.br"
    modalidade = context.get('modalidade', '').strip().lower()
    tipo = context.get('tipo', '').strip().lower()
    cidade = context.get('cidade', '').strip().lower() if context.get('cidade') else ''
    zona = context.get('zona', '').strip().lower() if context.get('zona') else ''
    bairro = context.get('bairro', '').strip().lower() if context.get('bairro') else ''
    local = context.get('local', '')

    # Mapeamento de tipologia para slug do Viva Real
    tipo_slug_map = {
        'apartamento': 'apartamento_residencial',
        'casa': 'casa_residencial',
        'casa de condomínio': "condominio_residencial",
        'cobertura residencial': "cobertura_residencial",
        'kitnet residencial': "kitnet_residencial",
        'flat residencial': "flat_residencial",
        'terreno': 'lote-terreno_residencial',
        'prédio residencial': 'edificio-residencial_comercial',
        'prédio comercial': 'predio_comercial',
        'sala comercial': 'sala_comercial',
        'galpões comerciais': "galpao_comercial",
        'pontos comerciais': "ponto-comercial_comercial",
        'consultórios comerciais': "consultorio_comercial",
        'imóveis comerciais': "imovel-comercial_comercial",
        'fazendas / sitios': 'granja_comercial'
    }
    tipo_slug = tipo_slug_map.get(tipo, tipo)

    # Modalidade
    trans_slug = 'venda' if modalidade == 'venda' else 'aluguel'

    # Mapeamento de zonas para slug do Viva Real
    zona_slug_map = {
        'zona sul': 'zona-sul',
        'zona norte': 'zona-norte',
        'zona oeste': 'zona-oeste',
        'centro': 'zona-central',
        'zona central': 'zona-central'
    }



    # Montagem da URL
    if local == 'todo_estado':
        # Todo o estado: /venda/rj/apartamento_residencial/
        url = f"{base_url}/{trans_slug}/rj/{tipo_slug}/"
    elif local == 'cidade':
        # Cidade do interior: /venda/rj/marica/casa_residencial/
        cidade_norm = normalize_str(cidade)
        url = f"{base_url}/{trans_slug}/rj/{cidade_norm}/{tipo_slug}/"
    elif local == 'bairro_interior':
        # Bairro de cidade do interior: /venda/rj/angra-dos-reis/bairros/centro/casa_residencial/
        cidade_norm = normalize_str(cidade)
        bairro_norm = normalize_str(bairro)
        url = f"{base_url}/{trans_slug}/rj/{cidade_norm}/bairros/{bairro_norm}/{tipo_slug}/"
    elif local == 'zona':
        # Zona completa: /venda/rj/rio-de-janeiro/zona-sul/
        zona_slug = zona_slug_map.get(zona, normalize_str(zona))
        if tipo_slug:
            url = f"{base_url}/{trans_slug}/rj/rio-de-janeiro/{zona_slug}/{tipo_slug}/"
        else:
            url = f"{base_url}/{trans_slug}/rj/rio-de-janeiro/{zona_slug}/"
    elif local == 'zona_completa':
        # Zona completa (nova funcionalidade): /venda/rj/rio-de-janeiro/zona-sul/casa_residencial/
        zona_slug = zona_slug_map.get(zona, normalize_str(zona))
        url = f"{base_url}/{trans_slug}/rj/rio-de-janeiro/{zona_slug}/{tipo_slug}/"
    elif local == 'bairro':
        # Bairro específico: /venda/rj/rio-de-janeiro/zona-sul/gloria/casa_residencial/
        zona_slug = zona_slug_map.get(zona, normalize_str(zona))
        bairro_norm = normalize_str(bairro)
        url = f"{base_url}/{trans_slug}/rj/rio-de-janeiro/{zona_slug}/{bairro_norm}/{tipo_slug}/"
    else:
        # fallback para cidade do RJ
        url = f"{base_url}/{trans_slug}/rj/rio-de-janeiro/{tipo_slug}/"
    
    logger.info(f"🔗 Generated URL: {url}")
    return url

# --- Scraping ---
def scrape_vivareal(url, refinamentos, max_pages=5, user_id=None, tipo_solicitado=None, tipo_transacao=None):
    logger.info(f"🕷️ Starting scraping: {url}, max_pages: {max_pages}, tipo: {tipo_solicitado}, transacao: {tipo_transacao}")
    data = []
    max_workers = min(4, max_pages)  # Limite de 4 threads para não sobrecarregar

    def scrape_page(page):
        # Verificar cancelamento no início de cada página
        if user_id and is_scraping_cancelled(user_id):
            logger.info(f"🚫 Scraping cancelled for user {user_id} on page {page}")
            return []
            
        options = ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1200")
        options.add_argument("--no-sandbox")
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')
        driver = None
        page_data = []
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            if page == 1:
                page_url = url
            else:
                page_url = f"{url}&pagina={page}" if '?' in url else f"{url}?pagina={page}"
            logger.info(f"📄 [Thread] Scraping page {page}: {page_url}")
            driver.get(page_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "li[data-cy='rp-property-cd'], div.results-list__container > p"))
            )
            
            # Verificar cancelamento após carregar a página
            if user_id and is_scraping_cancelled(user_id):
                logger.info(f"🚫 Scraping cancelled for user {user_id} after loading page {page}")
                return []
                
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            listings = soup.find_all('li', {'data-cy': 'rp-property-cd'})
            logger.info(f"🏠 [Thread] Found {len(listings)} properties on page {page}")
            for listing in listings:
                # Verificar cancelamento durante o processamento
                if user_id and is_scraping_cancelled(user_id):
                    logger.info(f"🚫 Scraping cancelled for user {user_id} during processing page {page}")
                    return page_data
                    
                try:
                    d = {}
                    # Adicionar Site
                    d['Site'] = 'Viva Real'
                    
                    # Adicionar tipo solicitado
                    d['Tipo de Imóvel'] = tipo_solicitado if tipo_solicitado else 'N/A'
                    
                    # Adicionar tipo de transação
                    d['Tipo de Transação'] = tipo_transacao if tipo_transacao else 'N/A'
                    
                    link_tag = listing.find('a', class_='block') if isinstance(listing, Tag) else None
                    d['Link'] = link_tag['href'] if isinstance(link_tag, Tag) and link_tag.has_attr('href') else 'N/A'
                    
                    # Extrair rua
                    street_p = listing.find('p', {'data-cy': 'rp-cardProperty-street-txt'}) if isinstance(listing, Tag) else None
                    d['Rua'] = street_p.get_text(strip=True) if isinstance(street_p, Tag) else 'N/A'
                    
                    # Extrair endereço completo
                    endereco_h2 = listing.find('h2', {'data-cy': 'rp-cardProperty-location-txt'}) if isinstance(listing, Tag) else None
                    endereco_completo = endereco_h2.get_text(strip=True) if isinstance(endereco_h2, Tag) else 'N/A'
                    d['Endereço'] = endereco_completo
                    
                    # Separar Estado, Município e Bairro do endereço
                    if endereco_completo != 'N/A':
                        # Formato típico: "Bairro, Cidade - Estado"
                        parts = endereco_completo.split(',')
                        if len(parts) >= 2:
                            bairro = parts[0].strip()
                            cidade_estado = parts[1].strip()
                            
                            # Separar cidade e estado
                            if ' - ' in cidade_estado:
                                cidade, estado = cidade_estado.split(' - ', 1)
                                d['Bairro'] = bairro
                                d['Município'] = cidade.strip()
                                d['Estado'] = estado.strip()
                            else:
                                d['Bairro'] = bairro
                                d['Município'] = cidade_estado
                                d['Estado'] = 'RJ'  # Padrão para Rio de Janeiro
                        else:
                            d['Bairro'] = endereco_completo
                            d['Município'] = 'Rio de Janeiro'
                            d['Estado'] = 'RJ'
                    else:
                        d['Bairro'] = 'N/A'
                        d['Município'] = 'N/A'
                        d['Estado'] = 'N/A'
                    
                    price_div = listing.find('div', {'data-cy': 'rp-cardProperty-price-txt'}) if isinstance(listing, Tag) else None
                    if isinstance(price_div, Tag):
                        paragraphs = price_div.find_all('p')
                        if paragraphs and isinstance(paragraphs[0], Tag):
                            d['Preço'] = paragraphs[0].get_text(strip=True)
                        else:
                            d['Preço'] = 'N/A'
                        if len(paragraphs) > 1 and isinstance(paragraphs[1], Tag):
                            cond_iptu_text = paragraphs[1].get_text(strip=True)
                            cond_match = re.search(r'Cond\.\s*R\$\s*([\d\.,]+)', cond_iptu_text)
                            iptu_match = re.search(r'IPTU\s*R\$\s*([\d\.,]+)', cond_iptu_text)
                            d['Condomínio'] = cond_match.group(1) if cond_match else 'N/A'
                            d['IPTU'] = iptu_match.group(1) if iptu_match else 'N/A'
                    else:
                        d['Preço'] = 'N/A'
                        d['Condomínio'] = 'N/A'
                        d['IPTU'] = 'N/A'
                    
                    d['Área m²'] = extract_feature(listing, 'rp-cardProperty-propertyArea-txt')
                    d['Quartos'] = extract_feature(listing, 'rp-cardProperty-bedroomQuantity-txt')
                    d['Banheiros'] = extract_feature(listing, 'rp-cardProperty-bathroomQuantity-txt')
                    d['Vagas'] = extract_feature(listing, 'rp-cardProperty-parkingSpacesQuantity-txt')
                    
                    page_data.append(d)
                except Exception as e:
                    logger.warning(f"⚠️ [Thread] Error processing listing on page {page}: {str(e)}")
                    continue
        except TimeoutException:
            logger.warning(f"⚠️ [Thread] Timeout on page {page}, skipping")
        except Exception as e:
            logger.error(f"❌ [Thread] Error on page {page}: {str(e)}")
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            # Pequeno delay randômico para evitar bloqueio
            time.sleep(random.uniform(0.5, 1.5))
        return page_data

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {executor.submit(scrape_page, page): page for page in range(1, max_pages + 1)}
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            # Verificar cancelamento antes de processar cada resultado
            if user_id and is_scraping_cancelled(user_id):
                logger.info(f"🚫 Scraping cancelled for user {user_id} during thread processing")
                break
            try:
                result = future.result()
                data.extend(result)
            except Exception as e:
                logger.error(f"❌ [ThreadPool] Error on page {page}: {str(e)}")

    # Remover duplicados por link
    seen = set()
    unique_data = []
    for d in data:
        link = d.get('Link', '')
        if link and link not in seen:
            unique_data.append(d)
            seen.add(link)

    logger.info(f"✅ Scraping completed: {len(unique_data)} unique properties found")
    
    # APLICAR FILTROS ANTES DE RETORNAR - ESSA ERA A PARTE QUE ESTAVA FALTANDO!
    if refinamentos:
        logger.info(f"🔍 Applying filters: {refinamentos}")
        filtered_data = apply_refinamentos(unique_data, refinamentos)
        logger.info(f"🔍 After filtering: {len(filtered_data)} properties remaining (from {len(unique_data)})")
        return filtered_data
    
    return unique_data

def extract_feature(listing, data_cy_value):
    el = listing.find(attrs={'data-cy': data_cy_value}) if isinstance(listing, Tag) else None
    if isinstance(el, Tag):
        text = el.get_text(strip=True)
        m = re.search(r'(\d+)', text)
        return m.group(1) if m else text
    return 'N/A'

def apply_refinamentos(data, refinamentos):
    """
    Aplica filtros aos dados coletados, baseado na lógica do DONE.py
    """
    if not refinamentos:
        return data
    
    def clean_to_float(text):
        """Converte texto para float, removendo caracteres não numéricos"""
        if not isinstance(text, str) or text == 'N/A': 
            return None
        try:
            # Remove tudo exceto dígitos e vírgulas, depois substitui vírgula por ponto
            cleaned = re.sub(r'[^\d,]', '', text).replace(',', '.')
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None

    def clean_to_int(text):
        """Converte texto para int, removendo caracteres não numéricos"""
        if not isinstance(text, str) or text == 'N/A': 
            return 0
        try:
            # Remove tudo exceto dígitos
            cleaned = re.sub(r'[^\d]', '', text)
            return int(cleaned) if cleaned else 0
        except (ValueError, TypeError):
            return 0
    
    logger.info(f"🔍 Applying filters to {len(data)} properties: {refinamentos}")
    
    # Extrair valores dos filtros com defaults seguros
    min_quartos = refinamentos.get('min_quartos', 0) or 0
    min_banheiros = refinamentos.get('min_banheiros', 0) or 0
    min_vagas = refinamentos.get('min_vagas', 0) or 0
    min_preco = refinamentos.get('min_preco')
    max_preco = refinamentos.get('max_preco')
    min_area = refinamentos.get('min_area')
    max_area = refinamentos.get('max_area')
    paga_condominio = refinamentos.get('paga_condominio', False)
    
    # Detectar se é busca apenas por terreno (baseado no tipo de imóvel)
    is_terreno_only = False
    for item in data[:5]:  # Verifica os primeiros 5 itens para determinar o tipo
        tipo_imovel = item.get('Tipo de Imóvel', '').lower()
        if 'terreno' in tipo_imovel:
            is_terreno_only = True
            break
    
    filtered_data = []
    original_count = len(data)
    
    for item in data:
        # Extrair valores do item
        item_preco = clean_to_float(item.get("Preço", "0"))
        item_area = clean_to_float(item.get("Área m²", "0"))
        item_quartos = clean_to_int(item.get("Quartos", "0"))
        item_banheiros = clean_to_int(item.get("Banheiros", "0"))
        item_vagas = clean_to_int(item.get("Vagas", "0"))
        item_condominio = item.get("Condomínio", "N/A") != "N/A"
        
        # Aplicar filtros específicos por tipo de imóvel
        if is_terreno_only:
            # Para terrenos, apenas verificar condomínio se solicitado
            if paga_condominio and not item_condominio: 
                continue
        else:
            # Para outros tipos de imóveis, verificar quartos, banheiros e vagas
            if item_quartos < min_quartos: 
                continue
            if item_banheiros < min_banheiros: 
                continue
            if item_vagas < min_vagas: 
                continue

        # Filtros de preço (aplicam a todos os tipos)
        if min_preco is not None and item_preco is not None and item_preco < min_preco: 
            continue
        if max_preco is not None and item_preco is not None and item_preco > max_preco: 
            continue
            
        # Filtros de área (aplicam a todos os tipos)
        if min_area is not None and item_area is not None and item_area < min_area: 
            continue
        if max_area is not None and item_area is not None and item_area > max_area: 
            continue
        
        # Se chegou até aqui, o item passou em todos os filtros
        filtered_data.append(item)
    
    filtered_count = len(filtered_data)
    logger.info(f"🔍 Filtering complete: {filtered_count}/{original_count} properties passed filters")
    
    # Log detalhado dos filtros aplicados
    if original_count > 0:
        removed_count = original_count - filtered_count
        removal_percentage = (removed_count / original_count) * 100
        logger.info(f"🔍 Removed {removed_count} properties ({removal_percentage:.1f}%) by filters")
    
    return filtered_data

def gpt4o_parse_refinamento(resposta_usuario):
    """
    Usa o GPT-4o para interpretar a resposta do usuário sobre refinamento e retorna um dicionário de filtros.
    Aceita tanto formato estruturado quanto linguagem natural.
    """
    prompt = (
        f"O usuário respondeu: '{resposta_usuario}'.\n"
        "Extraia os filtros de busca de imóveis. Interprete linguagem natural como:\n"
        "- 'área máxima 350m²' → max_area: 350\n"
        "- 'no máximo 250 mil' → max_preco: 250000\n"
        "- 'pelo menos 2 quartos' → min_quartos: 2\n"
        "- 'entre 100 e 200 mil' → min_preco: 100000, max_preco: 200000\n"
        "- 'acima de 80m²' → min_area: 80\n"
        "- '2 vagas ou mais' → min_vagas: 2\n\n"
        "Retorne APENAS um dicionário Python com as chaves: min_area, max_area, min_preco, max_preco, min_quartos, min_banheiros, min_vagas.\n"
        "Use None para valores não especificados. Exemplo:\n"
        "{'min_area': None, 'max_area': 350, 'min_preco': None, 'max_preco': 250000, 'min_quartos': 2, 'min_banheiros': None, 'min_vagas': None}"
    )
    try:
        resposta = gpt4o_ask(prompt, system="Você é um assistente que interpreta filtros de busca de imóveis. Responda APENAS com um dicionário Python válido, sem explicações.")
        logger.info(f"🤖 GPT-4o parsed refinamento: {resposta}")
        # Avaliar resposta como dicionário
        filtros = eval(resposta.strip(), {"__builtins__": {}})
        if isinstance(filtros, dict):
            return filtros
    except Exception as e:
        logger.error(f"❌ Erro ao interpretar refinamento: {str(e)} | Resposta: {resposta}")
    return {}

def enrich_property_details(properties, max_workers=6, user_id=None):
    """
    Após a coleta inicial, extrai dados detalhados de cada anúncio usando multi-threading.
    Adaptado do DONE.py para o contexto do bot Telegram.
    """
    logger.info(f"🔎 Starting enrichment for {len(properties)} properties")
    
    if not properties:
        logger.warning("⚠️ Nenhum imóvel encontrado para enriquecimento")
        return []
    
    # Filtrar apenas imóveis com links válidos
    valid_properties = []
    valid_links = []
    
    for prop in properties:
        link = prop.get("Link", "")
        if link and link != "N/A" and link.startswith("http"):
            valid_properties.append(prop)
            valid_links.append(link)
        else:
            # Para imóveis sem link válido, preencher com N/A e adicionar à lista final
            prop.update({
                'Anunciante': 'N/A',
                'Creci': 'N/A',
                'Classificacao_Anunciante': 'N/A',
                'Imoveis_Cadastrados': 'N/A',
                'Titulo_Anuncio': 'N/A',
                'Codigos_Anuncio': 'N/A',
                'Descricao': 'N/A',
                'Telefone': 'N/A',
                'Data_Criacao': 'N/A',
                'Endereco_Completo': 'N/A'
            })
            logger.warning(f"[ENRICH] Link inválido ignorado: {link}")
    
    if not valid_links:
        logger.warning("⚠️ Nenhum link válido encontrado para extração de detalhes")
        return properties  # Retorna os imóveis com N/A preenchido
    
    logger.info(f"🔗 Encontrados {len(valid_links)} links válidos para enriquecimento")
    
    # Extrai dados detalhados usando a função multi-threaded
    # max_workers: 6 threads por padrão (pode ser ajustado conforme capacidade do PC)
    # - Mais threads = Mais rápido, mas mais uso de CPU/RAM
    # - Recomendado: 4-8 threads para PCs normais, 8-12 para PCs potentes
    detailed_data = Extract_ad_info(valid_links, max_workers, user_id)
    
    # Adiciona os dados extraídos aos imóveis correspondentes
    enriched = []
    
    # Processar imóveis válidos
    for prop in valid_properties:
        link = prop.get("Link")
        if link in detailed_data:
            # Adiciona os novos campos ao dicionário do imóvel
            prop.update(detailed_data[link])
            logger.info(f"[ENRICH] Sucesso: {link}")
        else:
            # Se não encontrou dados, preenche com N/A
            prop.update({
                'Anunciante': 'N/A',
                'Creci': 'N/A',
                'Classificacao_Anunciante': 'N/A',
                'Imoveis_Cadastrados': 'N/A',
                'Titulo_Anuncio': 'N/A',
                'Codigos_Anuncio': 'N/A',
                'Descricao': 'N/A',
                'Telefone': 'N/A',
                'Data_Criacao': 'N/A',
                'Endereco_Completo': 'N/A'
            })
            logger.warning(f"[ENRICH] Dados não encontrados: {link}")
        enriched.append(prop)
    
    # Adicionar imóveis com links inválidos (já com N/A preenchido)
    for prop in properties:
        link = prop.get("Link", "")
        if not link or link == "N/A" or not link.startswith("http"):
            enriched.append(prop)
    
    logger.info(f"🔎 Enriquecimento concluído para {len(enriched)} imóveis")
    return enriched

def Extract_ad_info(links, max_workers=6, user_id=None):
    """
    Extrai informações detalhadas de múltiplos anúncios usando multi-threading.
    Adaptado do DONE.py para o contexto do bot Telegram com melhor robustez.
    """
    total_links = len(links)
    extracted_data = {}
    
    def extract_single_ad(link):
        """Extrai dados de um único anúncio com melhor tratamento de erros"""
        logger.info(f"[ENRICH] Iniciando enriquecimento: {link}")
        
        # Verificar cancelamento
        if user_id and is_scraping_cancelled(user_id):
            logger.info(f"🚫 Enrichment cancelled for user {user_id} during fetch")
            return link, {
                'Anunciante': 'N/A', 'Creci': 'N/A', 'Classificacao_Anunciante': 'N/A',
                'Imoveis_Cadastrados': 'N/A', 'Titulo_Anuncio': 'N/A', 'Codigos_Anuncio': 'N/A',
                'Descricao': 'N/A', 'Telefone': 'N/A', 'Data_Criacao': 'N/A', 'Endereco_Completo': 'N/A'
            }
        
        # Configurações do Chrome mais robustas
        options = ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1200")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins")
        options.add_argument("--disable-images")  # Desabilitar imagens para evitar bugs de foto
        options.add_argument("--disable-javascript")  # Desabilitar JS desnecessário
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')
        
        driver = None
        try:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            
            # Timeout mais curto para evitar travamentos
            driver.set_page_load_timeout(15)  
            driver.implicitly_wait(3)
            
            driver.get(link)
            
            # Aguardar menos tempo para acelerar o processo
            time.sleep(0.5)
            html = driver.page_source
            
            # Parse do HTML para extrair os dados
            soup = BeautifulSoup(html, 'html.parser')
            
            # Inicializa dados do anúncio
            ad_data = {
                'Anunciante': 'N/A',
                'Creci': 'N/A',
                'Classificacao_Anunciante': 'N/A',
                'Imoveis_Cadastrados': 'N/A',
                'Titulo_Anuncio': 'N/A',
                'Codigos_Anuncio': 'N/A',
                'Descricao': 'N/A',
                'Telefone': 'N/A',
                'Data_Criacao': 'N/A',
                'Endereco_Completo': 'N/A'
            }
            
            # Extrair dados do anunciante de forma mais robusta
            try:
                # Procurar seção do anunciante
                advertiser_section = soup.find('section', {'data-testid': 'advertiser-info-container'})
                if isinstance(advertiser_section, Tag):
                    # Nome do anunciante - tentar múltiplos seletores
                    name_tag = (advertiser_section.find('a', {'data-testid': 'official-store-redirect-link'}) or
                               advertiser_section.find('h3') or
                               advertiser_section.find('span', class_='advertiser-name'))
                    if isinstance(name_tag, Tag):
                        ad_data['Anunciante'] = name_tag.get_text(strip=True)
                    
                    # Creci - buscar em parágrafos
                    for p in advertiser_section.find_all('p'):
                        if isinstance(p, Tag):
                            text = p.get_text(strip=True)
                            if 'creci' in text.lower():
                                ad_data['Creci'] = text
                                break
                    
                    # Avaliação - buscar de forma mais simples
                    rating_div = advertiser_section.find('div', string=re.compile(r'\d+/\d+'))
                    if isinstance(rating_div, Tag):
                        ad_data['Classificacao_Anunciante'] = rating_div.get_text(strip=True)
                    
                    # Quantidade de imóveis - buscar números
                    for element in advertiser_section.find_all(['p', 'span', 'div']):
                        if isinstance(element, Tag):
                            text = element.get_text(strip=True)
                            if 'imóve' in text.lower() or 'propriedade' in text.lower():
                                numbers = re.search(r'(\d+(?:\.\d+)?)', text)
                                if numbers:
                                    ad_data['Imoveis_Cadastrados'] = numbers.group(1)
                                    break
            except Exception as e:
                logger.warning(f"[ENRICH] Erro ao extrair dados do anunciante: {str(e)}")
            
            # Extrair dados do anúncio de forma mais robusta
            try:
                # Título - tentar múltiplos seletores
                title_tag = (soup.find('h1', {'class': 'section-title'}) or
                            soup.find('h1') or
                            soup.find('title'))
                if isinstance(title_tag, Tag):
                    ad_data['Titulo_Anuncio'] = title_tag.get_text(strip=True)
                
                # Códigos do anúncio
                code_tag = soup.find('p', {'data-cy': 'ldp-propertyCodes-txt'})
                if isinstance(code_tag, Tag):
                    ad_data['Codigos_Anuncio'] = code_tag.get_text(strip=True)
                
                # Descrição - buscar em seção de descrição
                desc_section = soup.find('section', {'data-testid': 'description-container'})
                if isinstance(desc_section, Tag):
                    desc_tag = desc_section.find('p', {'data-testid': 'description-content'})
                    if isinstance(desc_tag, Tag):
                        ad_data['Descricao'] = desc_tag.get_text(strip=True)
                
                # Telefone - buscar de forma mais ampla
                phone_div = soup.find('div', {'data-testid': 'info-phone'})
                if isinstance(phone_div, Tag):
                    phone_span = phone_div.find('span')
                    if isinstance(phone_span, Tag):
                        ad_data['Telefone'] = phone_span.get_text(strip=True)
                
                # Endereço completo com número - buscar pelo seletor específico
                address_p = None
                
                # Tentar múltiplos seletores para encontrar o endereço completo
                try:
                    # Seletor exato fornecido pelo usuário
                    address_p = soup.find('p', {
                        'class': 'l-text l-u-color-neutral-28 l-text--variant-body-regular l-text--weight-bold address-info-value',
                        'data-testid': 'address-info-value'
                    })
                    
                    if isinstance(address_p, Tag):
                        address_text = address_p.get_text(strip=True)
                        if address_text and len(address_text) > 10:
                            ad_data['Endereco_Completo'] = address_text
                            logger.info(f"[ENRICH] Endereço completo extraído: {ad_data['Endereco_Completo']}")
                    
                    # Se não encontrou, tentar seletor mais simples
                    if not address_p or ad_data['Endereco_Completo'] == 'N/A':
                        address_p = soup.find('p', {'data-testid': 'address-info-value'})
                        if isinstance(address_p, Tag):
                            address_text = address_p.get_text(strip=True)
                            if address_text and len(address_text) > 10:
                                ad_data['Endereco_Completo'] = address_text
                                logger.info(f"[ENRICH] Endereço completo (fallback): {ad_data['Endereco_Completo']}")
                    
                    # Se ainda não encontrou, tentar busca por classe
                    if not address_p or ad_data['Endereco_Completo'] == 'N/A':
                        address_p = soup.find('p', {'class': 'address-info-value'})
                        if isinstance(address_p, Tag):
                            address_text = address_p.get_text(strip=True)
                            if address_text and len(address_text) > 10:
                                ad_data['Endereco_Completo'] = address_text
                                logger.info(f"[ENRICH] Endereço completo (classe): {ad_data['Endereco_Completo']}")
                
                except Exception as e:
                    logger.warning(f"[ENRICH] Erro ao extrair endereço: {str(e)}")
                
                # Se não encontrou com nenhum seletor, tentar busca mais ampla
                if not address_p or ad_data['Endereco_Completo'] == 'N/A':
                    # Buscar por qualquer elemento que contenha endereço
                    for element in soup.find_all(['p', 'div', 'span']):
                        if isinstance(element, Tag):
                            text = element.get_text(strip=True)
                            # Verificar se o texto parece ser um endereço (contém vírgula e números)
                            if (',' in text and 
                                any(char.isdigit() for char in text) and 
                                len(text) > 15 and 
                                ('rio de janeiro' in text.lower() or 'rj' in text.lower())):
                                ad_data['Endereco_Completo'] = text
                                logger.info(f"[ENRICH] Endereço completo (busca ampla): {ad_data['Endereco_Completo']}")
                                break
                
                # Data de criação
                date_span = soup.find('span', {'data-testid': 'listing-created-date'})
                if isinstance(date_span, Tag):
                    date_text = date_span.get_text(strip=True)
                    created_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', date_text)
                    if created_match:
                        ad_data['Data_Criacao'] = created_match.group(1)
                    else:
                        ad_data['Data_Criacao'] = date_text
                        
            except Exception as e:
                logger.warning(f"[ENRICH] Erro ao extrair dados do anúncio: {str(e)}")
            
            logger.info(f"[ENRICH] Sucesso: {link}")
            return link, ad_data
            
        except Exception as e:
            logger.error(f"❌ Error extracting data from {link}: {str(e)}")
            return link, {
                'Anunciante': 'N/A',
                'Creci': 'N/A',
                'Classificacao_Anunciante': 'N/A',
                'Imoveis_Cadastrados': 'N/A',
                'Titulo_Anuncio': 'N/A',
                'Codigos_Anuncio': 'N/A',
                'Descricao': 'N/A',
                'Telefone': 'N/A',
                'Data_Criacao': 'N/A'
            }
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            # Delay mais curto para acelerar o processo
            time.sleep(random.uniform(0.2, 0.5))
    
    # Usa ThreadPoolExecutor para processar múltiplos anúncios simultaneamente
    # Permitir mais threads para melhor performance, mas com limite de segurança
    max_workers = min(max_workers, total_links, 4)  # Máximo de 12 workers para PCs potentes
    completed_count = 0
    
    logger.info(f"🔎 Starting enrichment with {max_workers} workers for {total_links} links")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submete todas as tarefas
        future_to_link = {executor.submit(extract_single_ad, link): link for link in links}
        
        # Processa os resultados conforme são concluídos
        for future in as_completed(future_to_link):
            if user_id and is_scraping_cancelled(user_id):
                logger.info(f"🚫 Enrichment cancelled for user {user_id} during processing")
                break
                
            completed_count += 1
            try:
                link, ad_data = future.result(timeout=30)  # Timeout por thread
                extracted_data[link] = ad_data
                logger.info(f"✅ Progresso: {completed_count}/{total_links} - {link[:50]}...")
            except Exception as e:
                logger.error(f"❌ Error in enrichment thread: {str(e)}")
                link = future_to_link[future]
                extracted_data[link] = {
                    'Anunciante': 'N/A',
                    'Creci': 'N/A',
                    'Classificacao_Anunciante': 'N/A',
                    'Imoveis_Cadastrados': 'N/A',
                    'Titulo_Anuncio': 'N/A',
                    'Codigos_Anuncio': 'N/A',
                    'Descricao': 'N/A',
                    'Telefone': 'N/A',
                    'Data_Criacao': 'N/A',
                    'Endereco_Completo': 'N/A'
                }
    
    logger.info(f"🔎 Extração concluída! {len(extracted_data)} anúncios processados")
    return extracted_data

# --- Funções de controle ---
def cancel_user_scraping(user_id):
    """Cancela o scraping ativo para um usuário específico"""
    with scraping_lock:
        if user_id in active_scraping_tasks:
            active_scraping_tasks[user_id]['cancelled'] = True
            logger.info(f"🚫 Cancelled scraping for user {user_id}")
            return True
        return False

def is_scraping_cancelled(user_id):
    """Verifica se o scraping foi cancelado para um usuário"""
    with scraping_lock:
        if user_id in active_scraping_tasks:
            return active_scraping_tasks[user_id]['cancelled']
        return False

def register_scraping_task(user_id, thread):
    """Registra uma nova tarefa de scraping"""
    with scraping_lock:
        active_scraping_tasks[user_id] = {'cancelled': False, 'thread': thread}
        logger.info(f"📝 Registered scraping task for user {user_id}")

def unregister_scraping_task(user_id):
    """Remove o registro de uma tarefa de scraping"""
    with scraping_lock:
        if user_id in active_scraping_tasks:
            del active_scraping_tasks[user_id]
            logger.info(f"🗑️ Unregistered scraping task for user {user_id}")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /x - Cancela qualquer operação em andamento"""
    if not update.message:
        return ConversationHandler.END
        
    user_id = update.effective_user.id if update.effective_user else 0
    logger.info(f"🚫 User {user_id} requested cancellation with /x")
    
    # Cancela scraping se estiver ativo
    if cancel_user_scraping(user_id):
        await update.message.reply_text("❌ Operação cancelada! Use /start para começar uma nova busca.")
    else:
        await update.message.reply_text("ℹ️ Nenhuma operação em andamento para cancelar.")
    
    # Limpar completamente o contexto do usuário
    if hasattr(context, 'user_data') and context.user_data:
        context.user_data.clear()
    if hasattr(context, 'chat_data') and context.chat_data:
        context.chat_data.clear()
    
    return ConversationHandler.END

async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /r - Cancela operação atual e reinicia"""
    if not update.message:
        return ConversationHandler.END
        
    user_id = update.effective_user.id if update.effective_user else 0
    logger.info(f"🔄 User {user_id} requested restart with /r")
    
    # Cancela scraping se estiver ativo
    if cancel_user_scraping(user_id):
        await update.message.reply_text("🔄 Operação cancelada! Iniciando nova busca...")
    else:
        await update.message.reply_text("🔄 Iniciando nova busca...")
    
    # Limpar completamente o contexto do usuário
    if hasattr(context, 'user_data') and context.user_data:
        context.user_data.clear()
    if hasattr(context, 'chat_data') and context.chat_data:
        context.chat_data.clear()
    
    # Chama a função start para reiniciar
    return await start(update, context)

# --- Telegram Bot Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return ConversationHandler.END
        
    user_id = update.effective_user.id if update.effective_user else 0
    logger.info(f"👋 User {user_id} started the bot")
    
    # Limpar dados anteriores
    if isinstance(context.user_data, dict):
        context.user_data.clear()
    
    welcome_message = (
        " 🤖 Bem-vindo ao ImobBot!\n\n"
        "Vou te ajudar a encontrar imóveis no Rio de Janeiro através do Viva Real.\n\n"
        "• Use /start - Cancela e reinicia uma nova busca\n\n"
        "Vamos começar! Onde você quer buscar imóveis?\n\n"
        "1️⃣ Todo o estado do RJ - Busca em todo o Rio de Janeiro\n"
        "2️⃣ Bairros do Rio - Centro, Sul, Norte, Oeste \n"
        "3️⃣ Cidades do interior - Outras cidades do RJ\n"
        "4️⃣ Zonas do RJ Completas - Zonas inteiras sem bairros específicos\n"
        "5️⃣ Bairros de Cidades do Interior - RJ\n\n"
        "Responda apenas o número da opção (1, 2, 3, 4 ou 5)."
    )
    await update.message.reply_text(welcome_message)
    logger.info(f"📤 Sent to user {user_id}: {welcome_message[:100]}...")
    return ESCOLHA_LOCAL

async def escolha_local(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    
    user_id = update.effective_user.id if update.effective_user else 0
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose: {user_choice}")
    
    txt = update.message.text.strip()
    if txt == '1':
        if context.user_data is not None:
            context.user_data['local'] = 'todo_estado'
        logger.info(f"📍 User {user_id} selected: Todo o estado do RJ")
        return await pergunta_tipo(update, context)
    elif txt == '2':
        if context.user_data is not None:
            context.user_data['local'] = 'zona'
        zonas = list(ZONAS_RJ.keys())
        zonas_str = '\n'.join(f"{i+1}. {z}" for i, z in enumerate(zonas))
        pergunta = gpt4o_ask(
            f"O usuário escolheu buscar por zona/bairro da capital. Pergunte qual zona do Rio de Janeiro ele deseja, oferecendo as opções:\n{zonas_str}\nPeça para responder o número."
        )
        await update.message.reply_text(pergunta)
        logger.info(f"📤 Sent to user {user_id}: {pergunta[:100]}...")
        if context.user_data is not None:
            context.user_data['zonas'] = zonas
        return ESCOLHA_ZONA
    elif txt == '3':
        if context.user_data is not None:
            context.user_data['local'] = 'cidade'
        
        # Dividir a lista de cidades em duas partes para não sobrecarregar a mensagem
        total_cidades = len(CIDADES_RJ)
        metade = total_cidades // 2
        
        # Primeira metade
        cidades_parte1 = CIDADES_RJ[:metade]
        cidades_str1 = '\n'.join(f"{i+1}. {c}" for i, c in enumerate(cidades_parte1))
        
        # Segunda metade
        cidades_parte2 = CIDADES_RJ[metade:]
        cidades_str2 = '\n'.join(f"{i+metade+1}. {c}" for i, c in enumerate(cidades_parte2))
        
        # Enviar primeira mensagem
        await update.message.reply_text(
            f"🏙️ Qual cidade do interior do RJ você deseja?\n\n"
            f"**PARTE 1 ({total_cidades} cidades totais):**\n\n"
            f"{cidades_str1}\n\n"
            f"*Continua na próxima mensagem...*"
        )
        
        # Enviar segunda mensagem
        await update.message.reply_text(
            f"**PARTE 2:**\n\n"
            f"{cidades_str2}\n\n"
            f"*Responda apenas o número da cidade desejada.*"
        )
        
        logger.info(f"📤 Sent complete city list to user {user_id}")
        if context.user_data is not None:
            context.user_data['cidades'] = CIDADES_RJ  # Salvar lista completa
        return ESCOLHA_CIDADE
    elif txt == '4':
        context.user_data['local'] = 'zona_completa'
        zonas_completas = ['Zona Sul', 'Zona Norte', 'Zona Oeste', 'Zona Central']
        zonas_str = '\n'.join(f"{i+1}. {z}" for i, z in enumerate(zonas_completas))
        pergunta = (
            f"🎯 Qual zona completa do Rio de Janeiro você deseja buscar?\n\n"
            f"{zonas_str}\n\n"
            f"*Responda apenas o número da zona desejada.*"
        )
        await update.message.reply_text(pergunta)
        logger.info(f"📤 Sent complete zone options to user {user_id}")
        context.user_data['zonas_completas'] = zonas_completas
        return ESCOLHA_ZONA_COMPLETA
    elif txt == '5':
        if context.user_data is not None:
            context.user_data['local'] = 'cidade_interior'
        
        # Listar cidades do interior que têm bairros definidos
        cidades_interior = list(CIDADES_INTERIOR_BAIRROS.keys())
        total_cidades = len(cidades_interior)
        metade = total_cidades // 2
        
        # Primeira metade
        cidades_parte1 = cidades_interior[:metade]
        cidades_str1 = '\n'.join(f"{i+1}. {c}" for i, c in enumerate(cidades_parte1))
        
        # Segunda metade
        cidades_parte2 = cidades_interior[metade:]
        cidades_str2 = '\n'.join(f"{i+metade+1}. {c}" for i, c in enumerate(cidades_parte2))
        
        # Enviar primeira mensagem
        await update.message.reply_text(
            f"🏙️ Qual cidade do interior do RJ você deseja buscar bairros?\n\n"
            f"**PARTE 1 ({total_cidades} cidades com bairros disponíveis):**\n\n"
            f"{cidades_str1}\n\n"
            f"*Continua na próxima mensagem...*"
        )
        
        # Enviar segunda mensagem
        await update.message.reply_text(
            f"**PARTE 2:**\n\n"
            f"{cidades_str2}\n\n"
            f"*Responda apenas o número da cidade desejada.*"
        )
        
        logger.info(f"📤 Sent interior cities list to user {user_id}")
        if context.user_data is not None:
            context.user_data['cidades_interior'] = cidades_interior
        return ESCOLHA_CIDADE_INTERIOR
    else:
        await update.message.reply_text("Por favor, responda 1, 2, 3, 4 ou 5.")
        logger.info(f"❌ User {user_id} gave invalid choice: {txt}")
        return ESCOLHA_LOCAL

async def escolha_zona(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose zone: {user_choice}")
    
    try:
        zonas = context.user_data.get('zonas', [])
        if not isinstance(zonas, list):
            return ConversationHandler.END
        idx = int(update.message.text.strip()) - 1
        if idx < 0 or idx >= len(zonas):
            raise Exception
        zona = zonas[idx]
        context.user_data['zona'] = zona
        logger.info(f"📍 User {user_id} selected zone: {zona}")
        bairros = ZONAS_RJ[zona]
        bairros_str = '\n'.join(f"{i+1}. {b}" for i, b in enumerate(bairros))
        pergunta = gpt4o_ask(
            f"O usuário escolheu a zona '{zona}'. Pergunte se ele deseja buscar em algum bairro específico, mostrando as opções:\n{bairros_str}\nPeça para responder o número do bairro ou '0' para buscar em toda a zona."
        )
        await update.message.reply_text(pergunta)
        logger.info(f"📤 Sent to user {user_id}: {pergunta[:100]}...")
        context.user_data['bairros'] = bairros
        return ESCOLHA_BAIRRO
    except:
        await update.message.reply_text("Escolha inválida. Responda o número da zona.")
        logger.info(f"❌ User {user_id} gave invalid zone choice: {user_choice}")
        return ESCOLHA_ZONA

async def escolha_bairro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose neighborhood: {user_choice}")
    
    txt = update.message.text.strip()
    if txt == '0':
        context.user_data['local'] = 'zona'
        logger.info(f"📍 User {user_id} selected: entire zone")
        return await pergunta_tipo(update, context)
    try:
        bairros = context.user_data.get('bairros', [])
        if not isinstance(bairros, list):
            return ConversationHandler.END
        idx = int(txt) - 1
        if idx < 0 or idx >= len(bairros):
            raise Exception
        bairro = bairros[idx]
        context.user_data['bairro'] = bairro
        context.user_data['local'] = 'bairro'
        logger.info(f"📍 User {user_id} selected neighborhood: {bairro}")
        return await pergunta_tipo(update, context)
    except:
        await update.message.reply_text("Escolha inválida. Responda o número do bairro ou 0 para toda a zona.")
        logger.info(f"❌ User {user_id} gave invalid neighborhood choice: {user_choice}")
        return ESCOLHA_BAIRRO

async def escolha_cidade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose city: {user_choice}")
    
    try:
        # Tentar interpretar como número
        cidades = context.user_data.get('cidades', CIDADES_RJ)
        if not isinstance(cidades, list):
            return ConversationHandler.END
        
        # Debug: verificar se a lista está vazia
        if len(cidades) == 0:
            await update.message.reply_text(
                "❌ Lista de cidades não disponível. Use /restart para tentar novamente."
            )
            logger.error(f"❌ Empty cities list for user {user_id}")
            return ConversationHandler.END
        
        idx = int(user_choice) - 1
        if idx < 0 or idx >= len(cidades):
            raise ValueError("Índice fora do range")
        
        cidade = cidades[idx]
        context.user_data['cidade'] = cidade
        logger.info(f"📍 User {user_id} selected city: {cidade}")
        
        await update.message.reply_text(f"✅ Cidade selecionada: **{cidade}**")
        return await pergunta_tipo(update, context)
        
    except ValueError:
        # Se não conseguiu interpretar como número, tentar como nome exato
        cidade = user_choice
        if cidade not in CIDADES_RJ:
            total_cidades = len(CIDADES_RJ)
            await update.message.reply_text(
                f"❌ Opção inválida. Por favor, responda com o **número** da cidade desejada "
                f"(entre 1 e {total_cidades}), conforme mostrado na lista acima.\n\n"
                f"Ou digite o nome exato da cidade."
            )
            logger.info(f"❌ User {user_id} gave invalid city choice: {user_choice} (Total cities: {total_cidades})")
            return ESCOLHA_CIDADE
        
        # Se encontrou por nome exato
        context.user_data['cidade'] = cidade
        logger.info(f"📍 User {user_id} selected city by name: {cidade}")
        
        await update.message.reply_text(f"✅ Cidade selecionada: **{cidade}**")
        return await pergunta_tipo(update, context)

async def escolha_zona_completa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose complete zone: {user_choice}")
    
    try:
        zonas_completas = context.user_data.get('zonas_completas', [])
        if not isinstance(zonas_completas, list):
            return ConversationHandler.END
        idx = int(user_choice) - 1
        if idx < 0 or idx >= len(zonas_completas):
            raise Exception
        zona_completa = zonas_completas[idx]
        context.user_data['zona'] = zona_completa
        logger.info(f"📍 User {user_id} selected complete zone: {zona_completa}")
        
        await update.message.reply_text(f"✅ Zona selecionada: **{zona_completa}** (busca completa)")
        return await pergunta_tipo(update, context)
    except:
        await update.message.reply_text("Escolha inválida. Responda o número da zona desejada.")
        logger.info(f"❌ User {user_id} gave invalid zone choice: {user_choice}")
        return ESCOLHA_ZONA_COMPLETA

async def escolha_cidade_interior(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose interior city: {user_choice}")
    
    try:
        # Tentar interpretar como número
        cidades_interior = context.user_data.get('cidades_interior', [])
        if not isinstance(cidades_interior, list):
            return ConversationHandler.END
        
        # Debug: verificar se a lista está vazia
        if len(cidades_interior) == 0:
            await update.message.reply_text(
                "❌ Lista de cidades não disponível. Use /restart para tentar novamente."
            )
            logger.error(f"❌ Empty interior cities list for user {user_id}")
            return ConversationHandler.END
        
        idx = int(user_choice) - 1
        if idx < 0 or idx >= len(cidades_interior):
            raise ValueError("Índice fora do range")
        
        cidade_interior = cidades_interior[idx]
        context.user_data['cidade_interior'] = cidade_interior
        logger.info(f"📍 User {user_id} selected interior city: {cidade_interior}")
        
        # Buscar bairros da cidade selecionada
        bairros_cidade = CIDADES_INTERIOR_BAIRROS.get(cidade_interior, [])
        if not bairros_cidade:
            await update.message.reply_text(
                f"❌ Nenhum bairro encontrado para {cidade_interior}. Use /restart para tentar novamente."
            )
            logger.error(f"❌ No neighborhoods found for city {cidade_interior}")
            return ConversationHandler.END
        
        # Dividir lista de bairros se muito longa
        if len(bairros_cidade) > 20:
            metade = len(bairros_cidade) // 2
            bairros_parte1 = bairros_cidade[:metade]
            bairros_parte2 = bairros_cidade[metade:]
            
            bairros_str1 = '\n'.join(f"{i+1}. {b}" for i, b in enumerate(bairros_parte1))
            bairros_str2 = '\n'.join(f"{i+metade+1}. {b}" for i, b in enumerate(bairros_parte2))
            
            await update.message.reply_text(
                f"🏘️ Bairros de **{cidade_interior}** - PARTE 1:\n\n{bairros_str1}\n\n*Continua na próxima mensagem...*"
            )
            await update.message.reply_text(
                f"🏘️ Bairros de **{cidade_interior}** - PARTE 2:\n\n{bairros_str2}\n\n*Responda o número do bairro desejado.*"
            )
        else:
            bairros_str = '\n'.join(f"{i+1}. {b}" for i, b in enumerate(bairros_cidade))
            await update.message.reply_text(
                f"🏘️ Bairros de **{cidade_interior}**:\n\n{bairros_str}\n\n*Responda o número do bairro desejado.*"
            )
        
        logger.info(f"📤 Sent neighborhoods list for {cidade_interior} to user {user_id}")
        context.user_data['bairros_cidade_interior'] = bairros_cidade
        return ESCOLHA_BAIRRO_INTERIOR
        
    except ValueError:
        # Se não conseguiu interpretar como número, tentar como nome exato
        cidade_interior = user_choice
        if cidade_interior not in CIDADES_INTERIOR_BAIRROS:
            total_cidades = len(cidades_interior)
            await update.message.reply_text(
                f"❌ Opção inválida. Por favor, responda com o **número** da cidade desejada "
                f"(entre 1 e {total_cidades}), conforme mostrado na lista acima.\n\n"
                f"Ou digite o nome exato da cidade."
            )
            logger.info(f"❌ User {user_id} gave invalid interior city choice: {user_choice}")
            return ESCOLHA_CIDADE_INTERIOR
        
        # Se encontrou por nome exato
        context.user_data['cidade_interior'] = cidade_interior
        logger.info(f"📍 User {user_id} selected interior city by name: {cidade_interior}")
        
        # Buscar bairros da cidade selecionada
        bairros_cidade = CIDADES_INTERIOR_BAIRROS.get(cidade_interior, [])
        if not bairros_cidade:
            await update.message.reply_text(
                f"❌ Nenhum bairro encontrado para {cidade_interior}. Use /restart para tentar novamente."
            )
            logger.error(f"❌ No neighborhoods found for city {cidade_interior}")
            return ConversationHandler.END
        
        # Dividir lista de bairros se muito longa
        if len(bairros_cidade) > 20:
            metade = len(bairros_cidade) // 2
            bairros_parte1 = bairros_cidade[:metade]
            bairros_parte2 = bairros_cidade[metade:]
            
            bairros_str1 = '\n'.join(f"{i+1}. {b}" for i, b in enumerate(bairros_parte1))
            bairros_str2 = '\n'.join(f"{i+metade+1}. {b}" for i, b in enumerate(bairros_parte2))
            
            await update.message.reply_text(
                f"🏘️ Bairros de **{cidade_interior}** - PARTE 1:\n\n{bairros_str1}\n\n*Continua na próxima mensagem...*"
            )
            await update.message.reply_text(
                f"🏘️ Bairros de **{cidade_interior}** - PARTE 2:\n\n{bairros_str2}\n\n*Responda o número do bairro desejado.*"
            )
        else:
            bairros_str = '\n'.join(f"{i+1}. {b}" for i, b in enumerate(bairros_cidade))
            await update.message.reply_text(
                f"🏘️ Bairros de **{cidade_interior}**:\n\n{bairros_str}\n\n*Responda o número do bairro desejado.*"
            )
        
        logger.info(f"📤 Sent neighborhoods list for {cidade_interior} to user {user_id}")
        context.user_data['bairros_cidade_interior'] = bairros_cidade
        return ESCOLHA_BAIRRO_INTERIOR

async def escolha_bairro_interior(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose interior neighborhood: {user_choice}")
    
    try:
        # Tentar interpretar como número
        bairros_cidade = context.user_data.get('bairros_cidade_interior', [])
        if not isinstance(bairros_cidade, list):
            return ConversationHandler.END
        
        # Debug: verificar se a lista está vazia
        if len(bairros_cidade) == 0:
            await update.message.reply_text(
                "❌ Lista de bairros não disponível. Use /restart para tentar novamente."
            )
            logger.error(f"❌ Empty neighborhoods list for user {user_id}")
            return ConversationHandler.END
        
        idx = int(user_choice) - 1
        if idx < 0 or idx >= len(bairros_cidade):
            raise ValueError("Índice fora do range")
        
        bairro_interior = bairros_cidade[idx]
        context.user_data['bairro_interior'] = bairro_interior
        context.user_data['local'] = 'bairro_interior'
        context.user_data['cidade'] = context.user_data.get('cidade_interior')
        context.user_data['bairro'] = bairro_interior
        logger.info(f"📍 User {user_id} selected interior neighborhood: {bairro_interior}")
        
        cidade_interior = context.user_data.get('cidade_interior', 'N/A')
        await update.message.reply_text(f"✅ Selecionado: **{bairro_interior}**, {cidade_interior}")
        return await pergunta_tipo(update, context)
        
    except ValueError:
        # Se não conseguiu interpretar como número, tentar como nome exato
        bairro_interior = user_choice
        if bairro_interior not in bairros_cidade:
            total_bairros = len(bairros_cidade)
            await update.message.reply_text(
                f"❌ Opção inválida. Por favor, responda com o **número** do bairro desejado "
                f"(entre 1 e {total_bairros}), conforme mostrado na lista acima.\n\n"
                f"Ou digite o nome exato do bairro."
            )
            logger.info(f"❌ User {user_id} gave invalid interior neighborhood choice: {user_choice}")
            return ESCOLHA_BAIRRO_INTERIOR
        
        # Se encontrou por nome exato
        context.user_data['bairro_interior'] = bairro_interior
        context.user_data['local'] = 'bairro_interior'
        context.user_data['cidade'] = context.user_data.get('cidade_interior')
        context.user_data['bairro'] = bairro_interior
        logger.info(f"📍 User {user_id} selected interior neighborhood by name: {bairro_interior}")
        
        cidade_interior = context.user_data.get('cidade_interior', 'N/A')
        await update.message.reply_text(f"✅ Selecionado: **{bairro_interior}**, {cidade_interior}")
        return await pergunta_tipo(update, context)

async def pergunta_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str):
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    logger.info(f"🏠 Asking property type to user {user_id}")
    
    tipos = TIPOS_IMOVEL
    tipos_str = '\n'.join(f"{i+1}. {t}" for i, t in enumerate(tipos))
    
    await update.message.reply_text(
        f"🏠 Qual tipo de imóvel você procura?\n\n{tipos_str}\n\n"
        "*Responda apenas o número.*"
    )
    context.user_data['tipos'] = tipos
    return ESCOLHA_TIPO

async def escolha_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    user_choice = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose property type: {user_choice}")
    
    try:
        tipos = context.user_data.get('tipos', [])
        if not isinstance(tipos, list):
            return ConversationHandler.END
        idx = int(update.message.text.strip()) - 1
        if idx < 0 or idx >= len(tipos):
            raise Exception
        tipo = tipos[idx]
        context.user_data['tipo'] = tipo
        logger.info(f"🏠 User {user_id} selected property type: {tipo}")
        
        await update.message.reply_text(
            "💰 Você deseja:\n\n"
            "1️⃣ Alugar\n"
            "2️⃣ Comprar\n\n"
            "*Responda 1 ou 2.*"
        )
        return ESCOLHA_MODALIDADE
    except:
        await update.message.reply_text("Escolha inválida. Responda o número do tipo de imóvel.")
        logger.info(f"❌ User {user_id} gave invalid property type choice: {user_choice}")
        return ESCOLHA_TIPO

async def escolha_modalidade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    txt = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose transaction type: {txt}")
    
    if txt == '1':
        if isinstance(context.user_data, dict):
            context.user_data['modalidade'] = 'Aluguel'
            context.user_data['Tipo de Transação'] = 'Aluguel'
        logger.info(f"💰 User {user_id} selected: Aluguel")
    elif txt == '2':
        if isinstance(context.user_data, dict):
            context.user_data['modalidade'] = 'Venda'
            context.user_data['Tipo de Transação'] = 'Venda'
        logger.info(f"💰 User {user_id} selected: Venda")
    else:
        await update.message.reply_text("Responda 1 para Aluguel ou 2 para Venda.")
        logger.info(f"❌ User {user_id} gave invalid transaction type: {txt}")
        return ESCOLHA_MODALIDADE
    
    await update.message.reply_text(
        "🔍 Deseja filtrar sua busca?\n\n"
        "Você pode especificar:\n"
        "• Área (ex: até 100m²)\n"
        "• Preço (ex: até 500 mil)\n"
        "• Quartos, banheiros, vagas\n\n"
        "Responda com seus filtros ou digite 'não' para buscar sem filtros."
    )
    return ESCOLHA_REFINAMENTO

async def escolha_refinamento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    txt = update.message.text.strip().lower()
    logger.info(f"👤 User {user_id} refinement choice: {txt}")
    
    if txt in ['não', 'nao', 'n', 'nao quero', 'sem refinamento']:
        context.user_data['refinamentos'] = {}
        logger.info(f"🔍 User {user_id} chose: no refinements")
    else:
        # Usar GPT-4o para interpretar a resposta do usuário
        filtros = gpt4o_parse_refinamento(txt)
        if not filtros or not isinstance(filtros, dict):
            await update.message.reply_text(
                "🤔 Não entendi os filtros. Tente novamente com exemplos como:\n\n"
                "• área máxima 100m²\n"
                "• no máximo 500 mil\n"
                "• pelo menos 2 quartos\n"
                "• entre 200 e 400 mil\n"
                "• 2 quartos, 1 vaga\n\n"
                "Ou responda 'não' para buscar sem filtros."
            )
            logger.info(f"❌ User {user_id} gave unparseable refinement: {txt}")
            return ESCOLHA_REFINAMENTO
        context.user_data['refinamentos'] = {k: v for k, v in filtros.items() if v is not None}
        logger.info(f"🔍 User {user_id} refinements: {context.user_data['refinamentos']}")
    
    # Pergunta sobre número de páginas
    await update.message.reply_text(
        "📄 Quantas páginas deseja coletar? (1-20)\n\n"
        "💡 Dica: Cada página tem ~20 imóveis"
    )
    return ESCOLHA_PAGINAS

async def escolha_paginas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    txt = update.message.text.strip()
    logger.info(f"👤 User {user_id} chose pages: {txt}")
    
    try:
        paginas = int(txt)
        if paginas < 1 or paginas > 20:
            await update.message.reply_text("Por favor, escolha um número entre 1 e 20 páginas.")
            logger.info(f"❌ User {user_id} gave invalid page number: {txt}")
            return ESCOLHA_PAGINAS
        context.user_data['paginas'] = paginas
        logger.info(f"📄 User {user_id} selected pages: {paginas}")
    except ValueError:
        await update.message.reply_text("Por favor, digite um número válido de páginas (1-20).")
        logger.info(f"❌ User {user_id} gave invalid page format: {txt}")
        return ESCOLHA_PAGINAS
    
    # Resumo direto da busca
    user_data = context.user_data if isinstance(context.user_data, dict) else {}
    tipo = user_data.get('tipo', 'N/A')
    modalidade = user_data.get('modalidade', 'N/A')
    local = user_data.get('local', 'N/A')
    refinamentos = user_data.get('refinamentos', {})
    
    resumo = f"📋 **Resumo da busca:**\n"
    resumo += f"• Tipo: {tipo}\n"
    resumo += f"• Modalidade: {modalidade}\n"
    
    if local == 'todo_estado':
        resumo += f"• Local: Todo o estado do RJ\n"
    elif local == 'zona':
        resumo += f"• Zona: {user_data.get('zona', 'N/A')}\n"
    elif local == 'zona_completa':
        resumo += f"• Zona Completa: {user_data.get('zona', 'N/A')}\n"
    elif local == 'bairro':
        resumo += f"• Bairro: {user_data.get('bairro', 'N/A')} ({user_data.get('zona', 'N/A')})\n"
    elif local == 'cidade':
        resumo += f"• Cidade: {user_data.get('cidade', 'N/A')}\n"
    
    if refinamentos:
        resumo += f"• Filtros: "
        filtros = []
        if refinamentos.get('max_area'):
            filtros.append(f"até {refinamentos['max_area']}m²")
        if refinamentos.get('min_area'):
            filtros.append(f"mín {refinamentos['min_area']}m²")
        if refinamentos.get('max_preco'):
            filtros.append(f"até R$ {refinamentos['max_preco']:,}".replace(',', '.'))
        if refinamentos.get('min_preco'):
            filtros.append(f"mín R$ {refinamentos['min_preco']:,}".replace(',', '.'))
        if refinamentos.get('min_quartos'):
            filtros.append(f"{refinamentos['min_quartos']}+ quartos")
        resumo += ", ".join(filtros) if filtros else "Nenhum"
        resumo += "\n"
    
    resumo += f"• Páginas: {paginas}\n\n"
    resumo += "Posso iniciar a coleta?"
    
    await update.message.reply_text(resumo)
    return CONFIRMA_BUSCA

async def confirma_busca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not isinstance(update.message.text, str) or not update.message.text:
        return ConversationHandler.END
    if not isinstance(context.user_data, dict):
        return ConversationHandler.END
    
    user_id = update.effective_user.id
    txt = update.message.text.strip().lower()
    logger.info(f"👤 User {user_id} confirmation: {txt}")
    
    if txt not in ['sim', 's', 'yes', 'y']:
        await update.message.reply_text("Busca cancelada. Use /start para começar de novo.")
        logger.info(f"❌ User {user_id} cancelled the search")
        return ConversationHandler.END
    await update.message.reply_text("Iniciando a coleta. Isso pode levar alguns minutos...")
    logger.info(f"🚀 Starting scraping for user {user_id}")
    
    # Obter o event loop da thread principal
    loop = asyncio.get_event_loop()
    threading.Thread(target=run_scraping_and_send, args=(update, context, loop)).start()
    return AGUARDA_SCRAPING

def run_scraping_and_send(update, context, loop):
    user_id = update.effective_user.id
    user_data = context.user_data if isinstance(context.user_data, dict) else {}
    url = build_vivareal_url(user_data)
    refinamentos = user_data.get('refinamentos', {})
    max_pages = user_data.get('paginas', 5)  # Padrão 5 páginas se não especificado
    
    logger.info(f"🕷️ Starting scraping for user {user_id} with URL: {url}, pages: {max_pages}")
    
    # Registrar a tarefa de scraping
    register_scraping_task(user_id, threading.current_thread())
    
    try:
        # Verificar se foi cancelado antes de começar
        if is_scraping_cancelled(user_id):
            logger.info(f"🚫 Scraping cancelled for user {user_id} before starting")
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text("❌ Operação cancelada pelo usuário."),
                loop
            )
            return
            
        data = scrape_vivareal(url, refinamentos, max_pages=max_pages, user_id=user_id, tipo_solicitado=user_data.get('tipo', 'N/A'), tipo_transacao=user_data.get('modalidade', 'N/A'))
        
        # Verificar se foi cancelado após o scraping inicial
        if is_scraping_cancelled(user_id):
            logger.info(f"🚫 Scraping cancelled for user {user_id} after initial scraping")
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text("❌ Operação cancelada pelo usuário."),
                loop
            )
            return
        
        if not data:
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text(
                    "❌ Nenhum imóvel compatível com sua busca.\n\n"
                    "Tente ajustar seus filtros ou fazer uma nova busca.\n\n"
                    "Use /start para uma nova busca."
                ),
                loop
            )
            logger.info(f"❌ No properties found for user {user_id}")
            return
        
        # Nova mensagem: "Encontrei alguma coisa..."
        asyncio.run_coroutine_threadsafe(
            update.message.reply_text(
                f"🎯 Encontrei alguma coisa! {len(data)} imóveis coletados.\n\n"
                f"Agora estou aplicando seus filtros e coletando detalhes..."
            ),
            loop
        )
        logger.info(f"🔎 Enriquecendo detalhes dos imóveis para user {user_id}")
        
        # Enriquecer detalhes
        enriched_data = enrich_property_details(data, max_workers=4, user_id=user_id)
        
        # Verificar se foi cancelado após o enriquecimento
        if is_scraping_cancelled(user_id):
            logger.info(f"🚫 Scraping cancelled for user {user_id} after enrichment")
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text("❌ Operação cancelada pelo usuário."),
                loop
            )
            return
        
        # Verificar se nenhum imóvel passou pelos filtros após enriquecimento
        if not enriched_data or len(enriched_data) == 0:
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text(
                    "❌ Nenhum imóvel compatível com sua busca.\n\n"
                    "Tente ajustar seus filtros ou fazer uma nova busca.\n\n"
                    "Use /start para uma nova busca."
                ),
                loop
            )
            logger.info(f"❌ No properties remaining after enrichment for user {user_id}")
            return
        
        # Criar planilha com os dados coletados
        df = pd.DataFrame(enriched_data)
        
        # Adicionar informações de localização baseadas na busca
        local_tipo = user_data.get('local', '')
        
        if local_tipo == 'bairro':
            # Se a busca foi por bairro específico, usar esse bairro
            bairro_busca = user_data.get('bairro', 'N/A')
            df['Bairro'] = bairro_busca
            df['Município'] = 'Rio de Janeiro'
            df['Estado'] = 'RJ'
        elif local_tipo == 'zona':
            # Se foi por zona, manter o bairro extraído do endereço
            # Bairro já vem preenchido do scraping
            df['Município'] = 'Rio de Janeiro'
            df['Estado'] = 'RJ'
        elif local_tipo == 'zona_completa':
            # Se foi por zona completa, manter o bairro extraído do endereço
            # Bairro já vem preenchido do scraping
            df['Município'] = 'Rio de Janeiro'
            df['Estado'] = 'RJ'
        elif local_tipo == 'cidade':
            # Se foi por cidade do interior
            cidade_busca = user_data.get('cidade', 'N/A')
            df['Município'] = cidade_busca
            df['Estado'] = 'RJ'
            # Bairro já vem preenchido do scraping
        elif local_tipo == 'todo_estado':
            # Se foi todo o estado, manter município e bairro extraídos
            df['Estado'] = 'RJ'
            # Bairro e Município já vêm preenchidos do scraping
        
        # Ordem e nomes das colunas igual ao DONE.py
        column_order = [
            'Site', 'Tipo de Imóvel', 'Tipo de Transação',
            'Titulo_Anuncio', 'Codigos_Anuncio',
            'Preço', 'Condomínio', 'IPTU',
            'Quartos', 'Banheiros', 'Vagas', 'Área m²',
            'Rua', 'Bairro', 'Município', 'Estado', 'Endereco_Completo',
            'Anunciante', 'Creci', 'Classificacao_Anunciante', 'Imoveis_Cadastrados',
            'Descricao', 'Telefone', 'Data_Criacao',
            'Link'
        ]
        
        # Remove a coluna Endereço do DataFrame (já temos Bairro, Município e Estado separados)
        if 'Endereço' in df.columns:
            df = df.drop(columns=['Endereço'])
        
        # Filtra apenas as colunas que existem no DataFrame
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        file_path = f"imoveis_{user_id}_{int(time.time())}.xlsx"
        df.to_excel(file_path, index=False)
        logger.info(f"📊 Excel file created: {file_path} with {len(enriched_data)} properties")
        
        # Verificar se foi cancelado antes de enviar o arquivo
        if is_scraping_cancelled(user_id):
            logger.info(f"🚫 Scraping cancelled for user {user_id} before sending file")
            try:
                os.remove(file_path)
            except:
                pass
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text("❌ Operação cancelada pelo usuário."),
                loop
            )
            return
        
        # Função para gerar descrição específica do local
        def get_local_description(user_data):
            local = user_data.get('local', 'N/A')
            if local == 'todo_estado':
                return "Todo o estado do RJ"
            elif local == 'zona':
                zona = user_data.get('zona', 'N/A')
                return f"{zona} (com bairros)"
            elif local == 'zona_completa':
                zona = user_data.get('zona', 'N/A')
                return f"{zona} (completa)"
            elif local == 'bairro':
                bairro = user_data.get('bairro', 'N/A')
                zona = user_data.get('zona', 'N/A')
                return f"Bairro {bairro}, {zona}"
            elif local == 'bairro_interior':
                bairro = user_data.get('bairro', 'N/A')
                cidade = user_data.get('cidade', 'N/A')
                return f"Bairro {bairro}, {cidade}"
            elif local == 'cidade':
                cidade = user_data.get('cidade', 'N/A')
                return f"Cidade {cidade}"
            else:
                return local

        # Aguardar 3 segundos antes de enviar o arquivo
        logger.info(f"⏳ Aguardando 3 segundos antes de enviar arquivo para user {user_id}")
        time.sleep(3)
        
        # Enviar arquivo como .xlsx - PRIMEIRA TENTATIVA
        try:
            logger.info(f"📤 Tentativa 1: Enviando arquivo para user {user_id}")
            
            # Verificar se o arquivo existe
            if not os.path.exists(file_path):
                logger.error(f"❌ Arquivo não encontrado: {file_path}")
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            # Verificar tamanho do arquivo
            file_size = os.path.getsize(file_path)
            logger.info(f"📁 Tamanho do arquivo: {file_size} bytes")
            
            # Usar InputFile para forçar o envio
            with open(file_path, 'rb') as file:
                input_file = InputFile(file, filename=f"imoveis_rj_{len(enriched_data)}_imoveis.xlsx")
                
                asyncio.run_coroutine_threadsafe(
                    update.message.reply_document(
                        document=input_file,
                        caption=f"✅ Busca finalizada! {len(enriched_data)} imóveis encontrados.\n\n📊 Dados coletados:\n• Local: {get_local_description(user_data)}\n• Tipo: {user_data.get('tipo', 'N/A')}\n• Modalidade: {user_data.get('modalidade', 'N/A')}\n• Páginas: {max_pages}\n\nUse /start para nova busca."
                    ),
                    loop
                )
            
            logger.info(f"✅ Arquivo enviado com sucesso na primeira tentativa para user {user_id}")
        except Exception as send_error:
            logger.error(f"❌ Erro na primeira tentativa de envio para user {user_id}: {str(send_error)}")
            
            # SEGUNDA TENTATIVA após 2 segundos
            try:
                logger.info(f"🔄 Tentativa 2: Aguardando 2 segundos e tentando novamente para user {user_id}")
                time.sleep(2)
                
                # Segunda tentativa usando caminho do arquivo
                asyncio.run_coroutine_threadsafe(
                    update.message.reply_document(
                        document=file_path,
                        filename=f"imoveis_rj_{len(enriched_data)}_imoveis.xlsx",
                        caption=f"✅ Busca finalizada! {len(enriched_data)} imóveis encontrados.\n\n📊 Dados coletados:\n• Local: {get_local_description(user_data)}\n• Tipo: {user_data.get('tipo', 'N/A')}\n• Modalidade: {user_data.get('modalidade', 'N/A')}\n• Páginas: {max_pages}\n\nUse /start para nova busca."
                    ),
                    loop
                )
                logger.info(f"✅ Arquivo enviado com sucesso na segunda tentativa para user {user_id}")
            except Exception as second_send_error:
                logger.error(f"❌ Erro na segunda tentativa de envio para user {user_id}: {str(second_send_error)}")
                
                # TERCEIRA TENTATIVA - Enviar como mensagem de texto com informações
                try:
                    logger.info(f"🔄 Tentativa 3: Enviando informações como texto para user {user_id}")
                    
                    info_message = (
                        f"✅ Busca finalizada! {len(enriched_data)} imóveis encontrados.\n\n"
                        f"📊 Dados coletados:\n"
                        f"• Local: {get_local_description(user_data)}\n"
                        f"• Tipo: {user_data.get('tipo', 'N/A')}\n"
                        f"• Modalidade: {user_data.get('modalidade', 'N/A')}\n"
                        f"• Páginas: {max_pages}\n\n"
                        f"❌ Erro ao enviar arquivo: {str(second_send_error)}\n\n"
                        f"Use /start para uma nova busca."
                    )
                    
                    asyncio.run_coroutine_threadsafe(
                        update.message.reply_text(info_message),
                        loop
                    )
                    logger.info(f"✅ Informações enviadas como texto para user {user_id}")
                except Exception as text_error:
                    logger.error(f"❌ Erro ao enviar texto para user {user_id}: {str(text_error)}")
                    logger.error(f"💥 Falha total na comunicação com user {user_id}")
        
        # Limpar arquivo temporário
        try:
            os.remove(file_path)
            logger.info(f"🗑️ Arquivo temporário removido: {file_path}")
        except Exception as cleanup_error:
            logger.warning(f"⚠️ Erro ao remover arquivo temporário {file_path}: {str(cleanup_error)}")
        
        # Limpar registro da tarefa
        unregister_scraping_task(user_id)
        logger.info(f"✅ Processo finalizado para user {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Error in scraping for user {user_id}: {str(e)}")
        try:
            asyncio.run_coroutine_threadsafe(
                update.message.reply_text(
                    f"❌ Ocorreu um erro durante a coleta: {str(e)}\n\nTente novamente ou use /start para começar de novo."
                ),
                loop
            )
        except Exception as send_error:
            logger.error(f"❌ Error sending error message to user {user_id}: {str(send_error)}")
    finally:
        # Sempre desregistrar a tarefa ao final
        unregister_scraping_task(user_id)

# --- Main ---
def main():
    if not TELEGRAM_TOKEN or not isinstance(TELEGRAM_TOKEN, str):
        raise RuntimeError('TELEGRAM_TOKEN não definido no .env!')
    if not OPENAI_API_KEY or not isinstance(OPENAI_API_KEY, str):
        raise RuntimeError('OPENAI_API_KEY não definido no .env!')
    
    logger.info("🤖 Starting ImobBot...")
    logger.info(f"🔑 Telegram Token: {TELEGRAM_TOKEN[:10]}...")
    logger.info(f"🔑 OpenAI API Key: {OPENAI_API_KEY[:10]}...")
    
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    # Adicionar handlers para comandos de controle
    app.add_handler(CommandHandler('x', cancel_command))
    app.add_handler(CommandHandler('r', restart_command))
    
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            ESCOLHA_LOCAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_local)],
            ESCOLHA_ZONA: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_zona)],
            ESCOLHA_BAIRRO: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_bairro)],
            ESCOLHA_CIDADE: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_cidade)],
            ESCOLHA_ZONA_COMPLETA: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_zona_completa)],
            ESCOLHA_CIDADE_INTERIOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_cidade_interior)],
            ESCOLHA_BAIRRO_INTERIOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_bairro_interior)],
            ESCOLHA_TIPO: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_tipo)],
            ESCOLHA_MODALIDADE: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_modalidade)],
            ESCOLHA_REFINAMENTO: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_refinamento)],
            ESCOLHA_PAGINAS: [MessageHandler(filters.TEXT & ~filters.COMMAND, escolha_paginas)],
            CONFIRMA_BUSCA: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirma_busca)],
            AGUARDA_SCRAPING: [],
        },
        fallbacks=[
            CommandHandler('start', start),
            CommandHandler('x', cancel_command),
            CommandHandler('r', restart_command)
        ],
        allow_reentry=True
    )
    app.add_handler(conv)
    logger.info("✅ ImobBot is running and ready!")
    app.run_polling()

if __name__ == "__main__":
    main()

