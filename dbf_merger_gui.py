import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import os
import logging
from dbf_merger import DBFMerger


class DBFMergerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DBF и MariaDB обработчик")
        self.root.geometry("800x650")

        self.setup_logging()
        self.setup_styles()
        self.create_widgets()
        self.dbf_merger = DBFMerger()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename='dbf_merger.log'
        )

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')

        style.configure('TFrame', background='#f0f0f0')
        style.configure('TLabel', background='#f0f0f0', font=('Arial', 10))
        style.configure('TButton', font=('Arial', 10), padding=5)
        style.configure('TEntry', font=('Arial', 10), padding=5)
        style.configure('TLabelframe', background='#f0f0f0')
        style.configure('TLabelframe.Label', background='#f0f0f0')

        style.map('TButton',
                  foreground=[('active', 'black'), ('!disabled', 'black')],
                  background=[('active', '#e0e0e0'), ('!disabled', '#f0f0f0')])

        style.configure('Accent.TButton', foreground='white', background='#0078d7')

    def create_widgets(self):
        # Main Frame
        main_frame = ttk.Frame(self.root, padding="15")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # DBF Section
        dbf_frame = ttk.LabelFrame(main_frame, text="DBF Файлы", padding=10)
        dbf_frame.pack(fill=tk.X, pady=5)

        ttk.Label(dbf_frame, text="Каталог с DBF:").grid(row=0, column=0, sticky=tk.W, pady=3)
        self.input_dir = tk.StringVar()
        ttk.Entry(dbf_frame, textvariable=self.input_dir, width=60).grid(row=0, column=1, padx=5)
        ttk.Button(dbf_frame, text="Обзор...", command=self.browse_input_dir).grid(row=0, column=2)

        ttk.Label(dbf_frame, text="Файл результатов DBF:").grid(row=1, column=0, sticky=tk.W, pady=3)
        self.dbf_output = tk.StringVar()
        ttk.Entry(dbf_frame, textvariable=self.dbf_output, width=60).grid(row=1, column=1, padx=5)
        ttk.Button(dbf_frame, text="Обзор...", command=lambda: self.browse_output_file(self.dbf_output)).grid(row=1,
                                                                                                              column=2)

        # DB Section
        db_frame = ttk.LabelFrame(main_frame, text="MariaDB Запросы", padding=10)
        db_frame.pack(fill=tk.X, pady=5)

        # DB Connection
        conn_frame = ttk.Frame(db_frame)
        conn_frame.pack(fill=tk.X, pady=5)

        ttk.Label(conn_frame, text="Хост:").grid(row=0, column=0, sticky=tk.W)
        self.db_host = tk.StringVar(value="localhost")
        ttk.Entry(conn_frame, textvariable=self.db_host, width=20).grid(row=0, column=1, padx=5, sticky=tk.W)

        ttk.Label(conn_frame, text="Пользователь:").grid(row=0, column=2, sticky=tk.W, padx=10)
        self.db_user = tk.StringVar(value="root")
        ttk.Entry(conn_frame, textvariable=self.db_user, width=20).grid(row=0, column=3, padx=5, sticky=tk.W)

        ttk.Label(conn_frame, text="Пароль:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.db_password = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.db_password, width=20, show="*").grid(row=1, column=1, padx=5,
                                                                                      sticky=tk.W)

        ttk.Label(conn_frame, text="База данных:").grid(row=1, column=2, sticky=tk.W, padx=10)
        self.db_name = tk.StringVar()
        ttk.Entry(conn_frame, textvariable=self.db_name, width=20).grid(row=1, column=3, padx=5, sticky=tk.W)

        # SQL Query
        ttk.Label(db_frame, text="SQL запрос (используйте %s для подстановки полиса и даты окончания события):").pack(anchor=tk.W)
        self.sql_query = tk.Text(db_frame, height=5, width=80, wrap=tk.WORD)
        self.sql_query.pack(fill=tk.X, pady=5)

        ttk.Label(db_frame, text="Файл результатов SQL:").pack(anchor=tk.W)
        self.db_output = tk.StringVar()
        output_frame = ttk.Frame(db_frame)
        output_frame.pack(fill=tk.X)
        ttk.Entry(output_frame, textvariable=self.db_output, width=60).pack(side=tk.LEFT, padx=5)
        ttk.Button(output_frame, text="Обзор...", command=lambda: self.browse_output_file(self.db_output)).pack(
            side=tk.LEFT)

        # Process Button
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=15)
        ttk.Button(
            btn_frame,
            text="Начать обработку",
            command=self.process_files,
            style='Accent.TButton'
        ).pack(ipadx=10, ipady=5)

        # Status Bar
        self.status_var = tk.StringVar(value="Готов к работе")
        status_bar = ttk.Label(
            main_frame,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padding=(5, 2)
        )
        status_bar.pack(fill=tk.X, pady=(5, 0))

    def browse_input_dir(self):
        dir_path = filedialog.askdirectory(title="Выберите каталог с DBF файлами")
        if dir_path:
            self.input_dir.set(dir_path)
            default_dbf = os.path.join(dir_path, "dbf_results.xlsx")
            default_db = os.path.join(dir_path, "sql_results.xlsx")
            self.dbf_output.set(default_dbf)
            self.db_output.set(default_db)

    def browse_output_file(self, target_var):
        initial_file = target_var.get() or "output.xlsx"
        file_path = filedialog.asksaveasfilename(
            title="Укажите файл для сохранения",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialfile=os.path.basename(initial_file),
            initialdir=os.path.dirname(initial_file) or os.path.expanduser("~")
        )
        if file_path:
            target_var.set(file_path)

    def process_files(self):
        # Validate inputs
        if not self.input_dir.get():
            messagebox.showerror("Ошибка", "Не выбран каталог с DBF файлами!")
            return

        if not self.dbf_output.get():
            messagebox.showerror("Ошибка", "Не указан файл для сохранения DBF результатов!")
            return

        db_params = {}
        if self.db_name.get():
            if not self.db_output.get():
                messagebox.showerror("Ошибка", "Не указан файл для сохранения SQL результатов!")
                return

            if not self.sql_query.get("1.0", tk.END).strip():
                messagebox.showerror("Ошибка", "Не указан SQL запрос!")
                return

            db_params = {
                'host': self.db_host.get(),
                'user': self.db_user.get(),
                'password': self.db_password.get(),
                'database': self.db_name.get(),
                'sql_query': self.sql_query.get("1.0", tk.END).strip()
            }

        try:
            self.status_var.set("Идет обработка...")
            self.root.update()

            success = self.dbf_merger.process_all(
                input_dir=self.input_dir.get(),
                dbf_output=self.dbf_output.get(),
                db_output=self.db_output.get(),
                db_params=db_params
            )

            if success:
                self.status_var.set("Обработка завершена успешно")
                messagebox.showinfo(
                    "Готово",
                    "Обработка завершена успешно!\n\n"
                    f"DBF результаты: {self.dbf_output.get()}\n"
                    f"SQL результаты: {self.db_output.get() if db_params else 'Не выполнялись'}"
                )
            else:
                self.status_var.set("Ошибка обработки")
                messagebox.showerror(
                    "Ошибка",
                    "В процессе обработки возникли ошибки. Проверьте лог-файл."
                )
        except Exception as e:
            self.status_var.set("Ошибка обработки")
            messagebox.showerror(
                "Ошибка",
                f"Произошла ошибка:\n{str(e)}\n\nПроверьте лог-файл для деталей."
            )
        finally:
            self.root.focus_force()


if __name__ == "__main__":
    root = tk.Tk()
    app = DBFMergerApp(root)
    root.mainloop()