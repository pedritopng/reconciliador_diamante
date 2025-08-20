import pandas as pd
import re
import io
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import locale
from openpyxl.styles import NamedStyle
import sys
import subprocess
import threading
import queue


# --- INSTRUÇÕES ---
# 1. Execute o script.
# 2. Uma interface gráfica será aberta.
# 3. Use os botões "Selecionar..." para escolher os relatórios.
# 4. Clique em "Gerar Relatório" para iniciar a análise.
# 5. Acompanhe o progresso na barra e na caixa de status.
# 6. Ao final, clique em "Abrir Relatório" para ver o resultado no Excel.

def limpar_valor(valor):
    """
    Converte um valor em formato de string (ex: "1.574,00") para um número float.
    """
    if isinstance(valor, str):
        valor_limpo = valor.replace('.', '').replace(',', '.')
        try:
            return float(valor_limpo)
        except (ValueError, TypeError):
            return None
    elif isinstance(valor, (int, float)):
        return float(valor)
    return None


def normalizar_documento(doc_str):
    """
    Normaliza o número do documento para um formato canônico para permitir a correspondência.
    Extrai o padrão 'num/num' ou 'num-num' mesmo que haja texto adicional.
    Ex: '58817/03-DME' se torna '58817/003'.
    """
    if not isinstance(doc_str, str):
        return str(doc_str)

    match = re.search(r'(\d+[\/-]\d+)', doc_str)
    if not match:
        return doc_str.strip()

    doc_str_norm = match.group(1).replace('-', '/')
    partes = doc_str_norm.split('/')
    if len(partes) == 2:
        principal, parcela = partes
        parcela_padded = parcela.zfill(3)
        return f"{principal.strip()}/{parcela_padded.strip()}"

    return doc_str.strip()


def processar_nosso_relatorio(caminho_arquivo):
    """
    Lê e processa o nosso relatório (agora o semiestruturado).
    """
    df = pd.read_csv(caminho_arquivo, header=None, encoding='latin-1', delimiter=';', on_bad_lines='warn',
                     engine='python')
    df.columns = [f'col_{i}' for i in range(df.shape[1])]
    dados_extraidos = []
    regex_recebimento_padrao = re.compile(r'Recebimento cfe Dpl\s+(.*?)\s+-\s+(.*)')
    regex_recebimento_alt = re.compile(r'Recebimento cfe Dpl\s+([\w\d\/-]+)-(.*)')
    regex_recebimento_space = re.compile(r'Recebimento cfe Dpl\s+([\w\d\/-]+(?:-[\w\d]+)?)\s+([A-Za-z].*)')
    regex_reembolso_com_doc = re.compile(r'Reembolso Duplicata\s+([\w\d\/-]+)')
    regex_reembolso_sem_doc = re.compile(r'^Reembolso Duplicata$')
    regex_desconto = re.compile(r'^DESCONTO DUPL CFE BORDERO$')
    regex_pagamento = re.compile(r'Pagamento cfe dpl\.\s+(.*?)-DIAMANTE.*')

    for index, row in df.iterrows():
        historico = str(row['col_0']).strip()
        valor_str = str(row.get('col_1', '0'))
        documento, sacado = None, None

        match = (regex_recebimento_padrao.search(historico) or
                 regex_recebimento_alt.search(historico) or
                 regex_recebimento_space.search(historico))
        if match:
            documento, sacado = match.group(1).strip(), match.group(2).strip()
        elif (match := regex_pagamento.search(historico)):
            documento, sacado = match.group(1).strip(), "N/A (Pagamento)"
        elif (match := regex_reembolso_com_doc.search(historico)):
            documento, sacado = match.group(1).strip(), "N/A (Reembolso)"
        elif regex_reembolso_sem_doc.search(historico):
            documento, sacado = f"REEMBOLSO_SEM_DOC_{index}", "N/A (Reembolso sem doc)"
        elif regex_desconto.search(historico):
            documento, sacado = f"DESCONTO_BORDERO_{index}", "N/A (Desconto Bordero)"
        else:
            documento, sacado = (historico, "N/A (Lançamento Genérico)") if historico else (
                f"LANCAMENTO_VAZIO_LINHA_{index}", "N/A")

        if documento and (valor := limpar_valor(valor_str)) is not None and valor > 0:
            dados_extraidos.append({'Documento': documento, 'Sacado_Nosso': sacado, 'Valor_Nosso': valor})

    return pd.DataFrame(dados_extraidos)


def processar_relatorio_diamante(caminho_arquivo):
    """
    Lê e processa o relatório da Diamante (agora o estruturado).
    """
    df = pd.read_csv(caminho_arquivo, encoding='latin-1', delimiter=',')
    df = df[['Documento', 'Sacado', 'Valor', 'Valor Pago']].rename(columns={
        'Valor': 'Valor_Original_Diamante', 'Valor Pago': 'Valor_Pago_Diamante', 'Sacado': 'Sacado_Diamante'
    })
    df['Valor_Original_Diamante'] = df['Valor_Original_Diamante'].apply(limpar_valor)
    df['Valor_Pago_Diamante'] = df['Valor_Pago_Diamante'].apply(limpar_valor)
    df['Documento'] = df['Documento'].astype(str).str.strip()
    return df


def gerar_relatorio_excel(df_nosso_agg, df_diamante_agg, df_comparativo, caminho_saida):
    """
    Gera um relatório Excel detalhado com a análise completa.
    """
    with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
        try:
            locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
        except locale.Error:
            try:
                locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
            except locale.Error:
                print("Aviso: Locale 'pt_BR' não encontrado.")

        # Cálculos e DataFrames
        df_ambos = df_comparativo[df_comparativo['_merge'] == 'both'].copy()
        df_ambos['Juros/Taxas (Diamante)'] = df_ambos['Valor_Pago_Diamante'] - df_ambos['Valor_Original_Diamante']
        df_ambos['Diferenca_Liquida'] = df_ambos['Valor_Pago_Diamante'] - df_ambos['Valor_Nosso']

        df_so_nosso = df_comparativo[df_comparativo['_merge'] == 'left_only'].copy()
        df_so_diamante = df_comparativo[df_comparativo['_merge'] == 'right_only'].copy()
        df_so_diamante['Juros/Taxas (Diamante)'] = df_so_diamante['Valor_Pago_Diamante'] - df_so_diamante['Valor_Original_Diamante']

        # Aba de Sumário
        sumario_data = {
            'Métrica': ['Documentos Únicos (Nosso Relatório)', 'Valor Total (Nosso)', '',
                        'Documentos Únicos (Diamante)', 'Valor Original (Diamante)', 'Valor Pago (Diamante)',
                        'Total Juros/Taxas (Diamante)', '', 'Documentos Correspondentes',
                        'Documentos com Diferença de Valor', 'Valor Total das Diferenças Líquidas', '',
                        'Documentos Apenas no Nosso Relatório', 'Valor Total (Apenas Nosso)', '',
                        'Documentos Apenas no Rel. Diamante', 'Valor Total (Apenas Diamante)', '', 'VALIDAÇÃO FINAL',
                        'Diferença Real (Total Pago Diamante - Total Nosso)',
                        'Diferença Calculada (Soma das Discrepâncias)'],
            'Valor': [df_nosso_agg['Documento'].nunique(), df_nosso_agg['Valor_Nosso'].sum(), None,
                      df_diamante_agg['Documento'].nunique(), df_diamante_agg['Valor_Original_Diamante'].sum(),
                      df_diamante_agg['Valor_Pago_Diamante'].sum(), df_diamante_agg['Juros/Taxas (Diamante)'].sum(), None,
                      len(df_ambos), len(df_ambos[df_ambos['Diferenca_Liquida'].abs() > 0.01]),
                      df_ambos['Diferenca_Liquida'].sum(), None, len(df_so_nosso),
                      df_so_nosso['Valor_Nosso'].sum(), None, len(df_so_diamante),
                      df_so_diamante['Valor_Pago_Diamante'].sum(), None, "SUCESSO" if abs(
                    (df_diamante_agg['Valor_Pago_Diamante'].sum() - df_nosso_agg['Valor_Nosso'].sum()) - (
                                df_ambos['Diferenca_Liquida'].sum() - df_so_nosso['Valor_Nosso'].sum() +
                                df_so_diamante['Valor_Pago_Diamante'].sum())) < 0.01 else "FALHA",
                      df_diamante_agg['Valor_Pago_Diamante'].sum() - df_nosso_agg['Valor_Nosso'].sum(),
                      df_ambos['Diferenca_Liquida'].sum() - df_so_nosso['Valor_Nosso'].sum() + df_so_diamante[
                          'Valor_Pago_Diamante'].sum()]
        }
        pd.DataFrame(sumario_data).to_excel(writer, sheet_name='Sumario_Conciliacao', index=False)

        # Abas de detalhes
        colunas_diferenca = ['Documento', 'Valor_Original_Diamante', 'Juros/Taxas (Diamante)', 'Valor_Pago_Diamante',
                             'Valor_Nosso', 'Diferenca_Liquida']
        df_ambos[df_ambos['Diferenca_Liquida'].abs() > 0.01][colunas_diferenca].to_excel(writer,
                                                                                         sheet_name='Diferencas_de_Valor',
                                                                                         index=False)

        df_so_nosso[['Documento', 'Sacado_Nosso', 'Valor_Nosso']].to_excel(writer,
                                                                          sheet_name='Apenas_no_Nosso_Relatorio',
                                                                          index=False)

        colunas_so_diamante = ['Documento', 'Sacado_Diamante', 'Valor_Original_Diamante', 'Juros/Taxas (Diamante)',
                               'Valor_Pago_Diamante']
        df_so_diamante[colunas_so_diamante].to_excel(writer, sheet_name='Apenas_no_Rel_Diamante', index=False)

        # Formatação e auto-ajuste
        workbook = writer.book
        currency_style = NamedStyle(name='currency_br', number_format='R$ #,##0.00')
        integer_style = NamedStyle(name='integer', number_format='#,##0')
        if 'currency_br' not in workbook.style_names:
            workbook.add_named_style(currency_style)
        if 'integer' not in workbook.style_names:
            workbook.add_named_style(integer_style)

        sheets_to_format = {
            'Diferencas_de_Valor': ['B', 'C', 'D', 'E', 'F'],
            'Apenas_no_Nosso_Relatorio': ['C'],
            'Apenas_no_Rel_Diamante': ['C', 'D', 'E']
        }
        for sheet_name, cols in sheets_to_format.items():
            ws = writer.sheets[sheet_name]
            ws.auto_filter.ref = ws.dimensions
            for col_letter in cols:
                for cell in ws[col_letter][1:]:
                    cell.style = 'currency_br'

        ws_sumario = writer.sheets['Sumario_Conciliacao']
        for cell in ws_sumario['B'][1:]:
            metric_cell = ws_sumario[f'A{cell.row}']
            if "Documentos" in str(metric_cell.value) or "Correspondentes" in str(metric_cell.value):
                cell.style = 'integer'
            elif isinstance(cell.value, (int, float)):
                cell.style = 'currency_br'

        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                max_length = max((len(str(cell.value)) for cell in column_cells if cell.value is not None), default=0)
                worksheet.column_dimensions[column_cells[0].column_letter].width = (max_length + 2)


class ReconciliationApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Reconciliador de Relatórios")
        self.root.geometry("600x400")

        self.nosso_path = tk.StringVar()
        self.diamante_path = tk.StringVar()
        self.output_path = ""
        self.thread_queue = queue.Queue()
        self.is_running = False

        # --- Widgets ---
        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # File selection
        ttk.Label(main_frame, text="Nosso Relatório (interno):").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Entry(main_frame, textvariable=self.nosso_path, state="readonly").grid(row=0, column=1, sticky="ew",
                                                                                      padx=5)
        ttk.Button(main_frame, text="Selecionar...", command=lambda: self.select_file(self.nosso_path,
                                                                                      "Selecione o Nosso Relatório (CSV semiestruturado)")).grid(
            row=0, column=2)

        ttk.Label(main_frame, text="Relatório Diamante (externo):").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(main_frame, textvariable=self.diamante_path, state="readonly").grid(row=1, column=1, sticky="ew", padx=5)
        ttk.Button(main_frame, text="Selecionar...", command=lambda: self.select_file(self.diamante_path,
                                                                                      "Selecione o relatório Diamante (CSV estruturado)")).grid(
            row=1, column=2)

        # Controls
        self.generate_button = ttk.Button(main_frame, text="Gerar Relatório", command=self.start_reconciliation_thread,
                                          state="disabled")
        self.generate_button.grid(row=2, column=0, columnspan=3, pady=10)

        self.progress_bar = ttk.Progressbar(main_frame, orient="horizontal", mode="determinate")
        self.progress_bar.grid(row=3, column=0, columnspan=3, sticky="ew", pady=5)

        self.log_text = tk.Text(main_frame, height=8, state="disabled", bg="#f0f0f0")
        self.log_text.grid(row=4, column=0, columnspan=3, sticky="nsew")

        self.open_button = ttk.Button(main_frame, text="Abrir Relatório Gerado", command=self.open_report,
                                      state="disabled")
        self.open_button.grid(row=5, column=0, columnspan=3, pady=10)

        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(4, weight=1)

    def select_file(self, path_var, title):
        filepath = filedialog.askopenfilename(parent=self.root, title=title)
        if filepath:
            path_var.set(filepath)
            self.check_paths()

    def check_paths(self):
        if self.nosso_path.get() and self.diamante_path.get():
            self.generate_button.config(state="normal")
        else:
            self.generate_button.config(state="disabled")

    def log_message(self, message):
        self.log_text.config(state="normal")
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state="disabled")

    def start_reconciliation_thread(self):
        if self.is_running:
            return

        self.is_running = True
        self.generate_button.config(state="disabled")
        self.open_button.config(state="disabled")
        self.progress_bar['value'] = 0
        self.log_text.config(state="normal")
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state="disabled")

        self.thread = threading.Thread(target=self.run_reconciliation)
        self.thread.daemon = True
        self.thread.start()
        self.root.after(100, self.check_thread)

    def check_thread(self):
        try:
            while True:
                message = self.thread_queue.get(block=False)
                if isinstance(message, tuple):
                    msg_type = message[0]
                    if msg_type == "progress":
                        self.progress_bar['value'] = message[1]
                        self.log_message(message[2])
                    elif msg_type == "done":
                        self.is_running = False
                        self.open_button.config(state="normal")
                        messagebox.showinfo("Sucesso", "Relatório de reconciliação gerado com sucesso!",
                                            parent=self.root)
                        return
                    elif msg_type == "error":
                        self.is_running = False
                        messagebox.showerror("Erro", message[1], parent=self.root)
                        return
        except queue.Empty:
            pass
        finally:
            if self.is_running:
                self.root.after(100, self.check_thread)
            else:
                self.generate_button.config(state="normal")
                self.progress_bar['value'] = 0

    def run_reconciliation(self):
        try:
            self.thread_queue.put(("progress", 10, "Processando nosso relatório..."))
            df_nosso = processar_nosso_relatorio(self.nosso_path.get())

            self.thread_queue.put(("progress", 30, "Processando relatório Diamante..."))
            df_diamante = processar_relatorio_diamante(self.diamante_path.get())

            self.thread_queue.put(("progress", 50, "Normalizando documentos..."))
            df_nosso['Documento_Norm'] = df_nosso['Documento'].apply(normalizar_documento)
            df_diamante['Documento_Norm'] = df_diamante['Documento'].apply(normalizar_documento)

            self.thread_queue.put(("progress", 60, "Agregando valores..."))
            df_nosso_agg = df_nosso.groupby('Documento_Norm').agg(Valor_Nosso=('Valor_Nosso', 'sum'),
                                                                  Sacado_Nosso=('Sacado_Nosso',
                                                                                   'first')).reset_index()
            df_diamante_agg = df_diamante.groupby('Documento_Norm').agg(Valor_Original_Diamante=('Valor_Original_Diamante', 'sum'),
                                                                        Valor_Pago_Diamante=('Valor_Pago_Diamante', 'sum'),
                                                                        Sacado_Diamante=('Sacado_Diamante', 'first')).reset_index()
            df_diamante_agg['Juros/Taxas (Diamante)'] = df_diamante_agg['Valor_Pago_Diamante'] - df_diamante_agg[
                'Valor_Original_Diamante']

            self.thread_queue.put(("progress", 70, "Cruzando informações..."))
            df_nosso_agg.rename(columns={'Documento_Norm': 'Documento'}, inplace=True)
            df_diamante_agg.rename(columns={'Documento_Norm': 'Documento'}, inplace=True)
            df_comparativo = pd.merge(df_nosso_agg, df_diamante_agg, on='Documento', how='outer', indicator=True)

            self.thread_queue.put(("progress", 80, "Gerando planilha Excel..."))
            pasta_saida = os.path.dirname(self.nosso_path.get())
            self.output_path = os.path.join(pasta_saida, "Relatorio_Conciliacao.xlsx")
            gerar_relatorio_excel(df_nosso_agg, df_diamante_agg, df_comparativo, self.output_path)

            self.thread_queue.put(("progress", 100, "Análise concluída."))
            self.thread_queue.put(("done",))
        except PermissionError:
            self.thread_queue.put(("error",
                                   f"Não foi possível salvar o arquivo '{os.path.basename(self.output_path)}'.\n\nVerifique se o arquivo não está aberto e tente novamente."))
        except Exception as e:
            self.thread_queue.put(("error", f"Ocorreu um erro inesperado:\n\n{e}"))

    def open_report(self):
        if self.output_path and os.path.exists(self.output_path):
            try:
                if sys.platform == "win32":
                    os.startfile(self.output_path)
                elif sys.platform == "darwin":
                    subprocess.call(['open', self.output_path])
                else:
                    subprocess.call(['xdg-open', self.output_path])
            except Exception as e:
                messagebox.showerror("Erro ao Abrir", f"Não foi possível abrir o arquivo automaticamente.\n\nErro: {e}",
                                     parent=self.root)
        else:
            messagebox.showwarning("Aviso", "O relatório ainda não foi gerado ou não foi encontrado.", parent=self.root)


if __name__ == "__main__":
    app_root = tk.Tk()
    app = ReconciliationApp(app_root)
    app_root.mainloop()
