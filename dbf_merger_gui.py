import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import logging
from threading import Thread
from dbf_merger import DBFMerger


class DBFMergerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("DBF и MariaDB обработчик")
        self.root.geometry("850x700")

        self.setup_logging()
        self.setup_styles()
        self.create_widgets()

        self.merger = DBFMerger()
        self.processing = False
        self.results = None

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename='dbf_merger.log'
        )

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TButton', font=('Arial', 10), padding=5)
        style.configure('Red.TButton', foreground='red')

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding=10)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # DBF Section
        dbf_frame = ttk.LabelFrame(main_frame, text="DBF файлы", padding=10)
        dbf_frame.pack(fill=tk.X, pady=5)

        ttk.Label(dbf_frame, text="Каталог с DBF:").grid(row=0, column=0, sticky=tk.W)
        self.input_dir = tk.StringVar()
        ttk.Entry(dbf_frame, textvariable=self.input_dir, width=60).grid(row=0, column=1, padx=5)
        ttk.Button(dbf_frame, text="Обзор...", command=self.browse_input_dir).grid(row=0, column=2)

        # DB Section
        db_frame = ttk.LabelFrame(main_frame, text="MariaDB", padding=10)
        db_frame.pack(fill=tk.X, pady=5)

        ttk.Label(db_frame, text="Хост:").grid(row=0, column=0, sticky=tk.W)
        self.db_host = tk.StringVar(value="localhost")
        ttk.Entry(db_frame, textvariable=self.db_host, width=20).grid(row=0, column=1, sticky=tk.W)

        ttk.Label(db_frame, text="Пользователь:").grid(row=0, column=2, sticky=tk.W, padx=10)
        self.db_user = tk.StringVar(value="root")
        ttk.Entry(db_frame, textvariable=self.db_user, width=20).grid(row=0, column=3, sticky=tk.W)

        ttk.Label(db_frame, text="Пароль:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.db_password = tk.StringVar()
        ttk.Entry(db_frame, textvariable=self.db_password, width=20, show="*").grid(row=1, column=1, sticky=tk.W)

        ttk.Label(db_frame, text="База данных:").grid(row=1, column=2, sticky=tk.W, padx=10)
        self.db_name = tk.StringVar()
        ttk.Entry(db_frame, textvariable=self.db_name, width=20).grid(row=1, column=3, sticky=tk.W)


        # Output Section
        output_frame = ttk.LabelFrame(main_frame, text="Результаты", padding=10)
        output_frame.pack(fill=tk.X, pady=5)

        ttk.Label(output_frame, text="Файл для сохранения:").grid(row=0, column=0, sticky=tk.W)
        self.output_file = tk.StringVar()
        ttk.Entry(output_frame, textvariable=self.output_file, width=60).grid(row=0, column=1, padx=5)
        ttk.Button(output_frame, text="Обзор...", command=self.browse_output_file).grid(row=0, column=2)

        # Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=10)

        self.process_btn = ttk.Button(btn_frame, text="Запустить обработку", command=self.start_processing)
        self.process_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(btn_frame, text="Остановить", command=self.stop_processing, style='Red.TButton',
                                   state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        self.compare_btn = ttk.Button(btn_frame, text="Сравнить результаты", command=self.compare_results,
                                      state=tk.DISABLED)
        self.compare_btn.pack(side=tk.LEFT, padx=5)

        # Status
        self.status = tk.StringVar(value="Готов к работе")
        ttk.Label(main_frame, textvariable=self.status, relief=tk.SUNKEN).pack(fill=tk.X, pady=5)

        # Log
        log_frame = ttk.LabelFrame(main_frame, text="Лог", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(log_frame, height=10, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(self.log_text)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.log_text.yview)

    def browse_input_dir(self):
        dir_path = filedialog.askdirectory(title="Выберите каталог с DBF файлами")
        if dir_path:
            self.input_dir.set(dir_path)
            self.output_file.set(os.path.join(dir_path, "result_comparison.xlsx"))

    def browse_output_file(self):
        file_path = filedialog.asksaveasfilename(
            title="Укажите файл для сохранения",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if file_path:
            self.output_file.set(file_path)

    def log_message(self, message):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)

    def start_processing(self):
        if not self.input_dir.get():
            messagebox.showerror("Ошибка", "Укажите каталог с DBF файлами!")
            return

        self.processing = True
        self.process_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.compare_btn.config(state=tk.DISABLED)
        self.status.set("Обработка...")
        self.log_message("Начало обработки")

        db_params = {
            'host': self.db_host.get(),
            'user': self.db_user.get(),
            'password': self.db_password.get(),
            'database': self.db_name.get(),
            'sql_query': self.sql_query.get("1.0", tk.END).strip()
        }

        Thread(target=self.run_processing, args=(db_params,), daemon=True).start()

    def run_processing(self, db_params):
        try:
            self.results = self.merger.process(self.input_dir.get(), db_params)

            if self.merger.should_stop:
                self.log_message("Обработка прервана пользователем")
                self.status.set("Обработка прервана")
            else:
                self.log_message("Обработка завершена успешно")
                self.status.set("Готово")
                self.compare_btn.config(state=tk.NORMAL)

        except Exception as e:
            self.log_message(f"Ошибка: {str(e)}")
            self.status.set("Ошибка обработки")
        finally:
            self.processing = False
            self.process_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)

    def stop_processing(self):
        self.merger.stop()
        self.status.set("Завершение...")
        self.log_message("Получена команда остановки...")

    def compare_results(self):
        if not self.results or self.output_file.get() == "":
            messagebox.showerror("Ошибка", "Нет данных для сравнения или не указан файл!")
            return

        try:
            self.status.set("Сравнение результатов...")
            self.log_message("Начато сравнение результатов")

            comparison = self.merger.comparator.compare_results(
                self.results['dbf'],
                self.results['db']
            )

            self.merger.comparator.save_comparison(
                comparison,
                self.output_file.get()
            )

            self.log_message(f"Результаты сохранены в {self.output_file.get()}")
            self.status.set("Сравнение завершено")

            messagebox.showinfo("Готово", "Сравнение результатов завершено!")

        except Exception as e:
            self.log_message(f"Ошибка сравнения: {str(e)}")
            self.status.set("Ошибка сравнения")
            messagebox.showerror("Ошибка", f"При сравнении возникла ошибка:\n{str(e)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = DBFMergerApp(root)
    root.mainloop()