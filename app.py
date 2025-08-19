from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd
from scripts.processamento_vendas import VendaProcessor
import os
from werkzeug.utils import secure_filename
import io
from config import Config

# Diretório para salvar arquivos temporários
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx'}

app = Flask(__name__)
app.config.from_object(Config)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_unique_filename(base_path, original_filename):
    name, extension = os.path.splitext(original_filename)
    counter = 1
    new_filename = original_filename
    while os.path.exists(os.path.join(base_path, new_filename)):
        new_filename = f"{name}({counter}){extension}"
        counter += 1
    return os.path.join(base_path, new_filename)

def validar_dados(data_inicial_str, data_final_str):
    if not data_inicial_str or not data_final_str:
        return "Por favor, preencha ambas as datas."
    try:
        data_inicial = pd.to_datetime(data_inicial_str, format="%d/%m/%Y")
        data_final = pd.to_datetime(data_final_str, format="%d/%m/%Y")
        if data_inicial > data_final:
            return "A data inicial não pode ser maior que a final."
        return data_inicial, data_final
    except ValueError:
        return "Datas inválidas. Use o formato dd/mm/aaaa."

@app.route('/vendas', methods=['GET', 'POST'])
def processar_vendas():
    if request.method == 'POST':
        data_inicial_str = request.form['data_inicial']
        data_final_str = request.form['data_final']
        validacao = validar_dados(data_inicial_str, data_final_str)
        
        if isinstance(validacao, str):
            flash(validacao, 'error')
            return render_template('vendas.html')
        
        data_inicial, data_final = validacao
        
        if 'cielo' not in request.files or 'vendas' not in request.files:
            flash('Um ou mais arquivos não foram enviados.', 'error')
            return render_template('vendas.html')
            
        file_cielo = request.files['cielo']
        file_vendas = request.files['vendas']
        
        if file_cielo.filename == '' or file_vendas.filename == '':
            flash('Por favor, selecione ambos os arquivos.', 'error')
            return render_template('vendas.html')

        if not allowed_file(file_cielo.filename) or not allowed_file(file_vendas.filename):
            flash('Apenas arquivos .xlsx são permitidos.', 'error')
            return render_template('vendas.html')

        path_cielo = None
        path_vendas = None
        try:
            filename_cielo = secure_filename(file_cielo.filename)
            path_cielo = os.path.join(app.config['UPLOAD_FOLDER'], filename_cielo)
            file_cielo.save(path_cielo)
            
            filename_vendas = secure_filename(file_vendas.filename)
            path_vendas = os.path.join(app.config['UPLOAD_FOLDER'], filename_vendas)
            file_vendas.save(path_vendas)

            processor = VendaProcessor(data_inicial, data_final, path_cielo, path_vendas)
            lista_tuplas_dfs, error = processor.processar()
            
            if error:
                flash(error, 'error')
            elif lista_tuplas_dfs is not None:
                consolidado = request.form.get('consolidado')
                
                if consolidado:
                    # Adiciona a linha 5112 e salva o arquivo consolidado
                    dfs = [df for data, df in lista_tuplas_dfs]
                    df_final = pd.concat(dfs, ignore_index=True)
                    
                    df_first_line = pd.DataFrame([["5112"] + ["" for _ in range(7)]], columns=df_final.columns)
                    df_final = pd.concat([df_first_line, df_final], ignore_index=True)
                    
                    nome_final = f"intervalo_consolidado_{data_inicial.strftime('%Y-%m-%d')}_a_{data_final.strftime('%Y-%m-%d')}.csv"
                    caminho_salvo = os.path.join(app.config['UPLOAD_FOLDER'], nome_final)
                    
                    try:
                        df_final.to_csv(caminho_salvo, index=False, header=False, sep=";", encoding="ISO-8859-1", errors="replace")
                        flash(f"Arquivo consolidado gerado com sucesso na pasta 'uploads'.", 'success')
                    except PermissionError:
                        new_path = generate_unique_filename(app.config['UPLOAD_FOLDER'], nome_final)
                        df_final.to_csv(new_path, index=False, header=False, sep=";", encoding="ISO-8859-1", errors="replace")
                        flash(f"O arquivo estava em uso. Uma nova versão foi salva como: {new_path}", 'warning')

                else:
                    # Salva arquivos diários
                    for data, df_dia in lista_tuplas_dfs:
                        df_first_line = pd.DataFrame([["5112"] + ["" for _ in range(7)]], columns=df_dia.columns)
                        df_dia_completo = pd.concat([df_first_line, df_dia], ignore_index=True)

                        nome_arquivo = f"{data}.csv"
                        caminho_salvo = os.path.join(app.config['UPLOAD_FOLDER'], nome_arquivo)
                        
                        try:
                            df_dia_completo.to_csv(caminho_salvo, index=False, header=False, sep=";", encoding="ISO-8859-1", errors="replace")
                        except PermissionError:
                            new_path = generate_unique_filename(app.config['UPLOAD_FOLDER'], nome_arquivo)
                            df_dia_completo.to_csv(new_path, index=False, header=False, sep=";", encoding="ISO-8859-1", errors="replace")
                    
                    flash(f"Arquivos diários gerados com sucesso na pasta 'uploads'.", 'success')

            else:
                flash("Nenhum dado processado. Verifique os arquivos e datas.", 'error')
                
        except Exception as e:
            flash(f"Ocorreu um erro no processamento: {e}", 'error')
            
        finally:
            if path_cielo and os.path.exists(path_cielo):
                os.remove(path_cielo)
            if path_vendas and os.path.exists(path_vendas):
                os.remove(path_vendas)
            
    return render_template('vendas.html')

@app.route('/')
def home():
    return render_template('index.html')