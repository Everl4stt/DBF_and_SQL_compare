import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import logging
from threading import Thread
from patient_search import PatientSearcher  # Импортируем новый класс
from dbf_merger import DBFMerger
from config import main_query


class DBFMergerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Vasyatkinator 3000 v1.2")
        self.root.geometry("1000x900")

        # Инициализация классов
        self.patient_searcher = PatientSearcher()
        self.processing = False

        self.setup_logging()
        self.setup_ui()
        self.create_results_dir()

    def create_results_dir(self):
        """Создает каталог results если его нет"""
        if not os.path.exists('results'):
            os.makedirs('results')

    def setup_logging(self):
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename='app.log'
        )

    def setup_ui(self):
        """Настройка пользовательского интерфейса"""
        # Создаем вкладки
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Вкладка основного функционала
        self.setup_main_tab()

        # Вкладка поиска пациентов
        self.setup_search_tab()

        # Общие элементы (статус бар и лог)
        self.setup_common_elements()

    def setup_main_tab(self):
        """Вкладка основного функционала"""
        main_tab = ttk.Frame(self.notebook)
        self.notebook.add(main_tab, text="Основной функционал")

        # Здесь размещаем ваш существующий основной функционал
        # ... (ваш текущий код для основной вкладки)

    def setup_search_tab(self):
        """Вкладка поиска пациентов"""
        search_tab = ttk.Frame(self.notebook)
        self.notebook.add(search_tab, text="Поиск пациента")

        # Основной фрейм
        search_frame = ttk.Frame(search_tab, padding=10)
        search_frame.pack(fill=tk.BOTH, expand=True)

        # Параметры поиска
        params_frame = ttk.LabelFrame(search_frame, text="Параметры поиска", padding=10)
        params_frame.pack(fill=tk.X, pady=5)

        # Поле для ввода ФИО
        ttk.Label(params_frame, text="Фамилия: ").grid(row=0, column=0, sticky=tk.W, padx=5)
        self.patient_lastname = tk.StringVar(value='Степченко')
        ttk.Entry(params_frame, textvariable=self.patient_lastname, width=25).grid(
            row=0, column=1, sticky=tk.W, padx=5)

        ttk.Label(params_frame, text="Имя:").grid(row=0, column=2, sticky=tk.W, padx=5)
        self.patient_firstname = tk.StringVar(value='Любовь')
        ttk.Entry(params_frame, textvariable=self.patient_firstname, width=25).grid(
            row=0, column=3, sticky=tk.W, padx=5)

        ttk.Label(params_frame, text="Отчество:").grid(row=0, column=4, sticky=tk.W, padx=5)
        self.patient_patrname = tk.StringVar(value='Алексеевна')
        ttk.Entry(params_frame, textvariable=self.patient_patrname, width=25).grid(
            row=0, column=5, sticky=tk.W, padx=5)

        # База данных 1
        db1_frame = ttk.LabelFrame(search_frame, text="Наша база данных", padding=10)
        db1_frame.pack(fill=tk.X, pady=5)

        ttk.Label(db1_frame, text="Хост:").grid(row=0, column=0, sticky=tk.W)
        self.db1_host = tk.StringVar(value="kkoddb")
        ttk.Entry(db1_frame, textvariable=self.db1_host, width=20).grid(
            row=0, column=1, sticky=tk.W, padx=5)

        ttk.Label(db1_frame, text="Пользователь:").grid(row=0, column=2, sticky=tk.W, padx=10)
        self.db1_user = tk.StringVar(value="dbuser")
        ttk.Entry(db1_frame, textvariable=self.db1_user, width=20).grid(
            row=0, column=3, sticky=tk.W)

        ttk.Label(db1_frame, text="Пароль:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.db1_password = tk.StringVar(value="dbpassword")
        ttk.Entry(db1_frame, textvariable=self.db1_password, width=20, show="*").grid(
            row=1, column=1, sticky=tk.W, padx=5)

        ttk.Label(db1_frame, text="База данных:").grid(row=1, column=2, sticky=tk.W, padx=10)
        self.db1_name = tk.StringVar(value="s12")
        ttk.Entry(db1_frame, textvariable=self.db1_name, width=20).grid(
            row=1, column=3, sticky=tk.W)

        # База данных 2
        db2_frame = ttk.LabelFrame(search_frame, text="База данных для сравнения", padding=10)
        db2_frame.pack(fill=tk.X, pady=5)

        ttk.Label(db2_frame, text="Хост:").grid(row=0, column=0, sticky=tk.W)
        self.db2_host = tk.StringVar(value="kkoddb")
        ttk.Entry(db2_frame, textvariable=self.db2_host, width=20).grid(
            row=0, column=1, sticky=tk.W, padx=5)

        ttk.Label(db2_frame, text="Пользователь:").grid(row=0, column=2, sticky=tk.W, padx=10)
        self.db2_user = tk.StringVar(value="dbuser")
        ttk.Entry(db2_frame, textvariable=self.db2_user, width=20).grid(
            row=0, column=3, sticky=tk.W)

        ttk.Label(db2_frame, text="Пароль:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.db2_password = tk.StringVar(value="dbpassword")
        ttk.Entry(db2_frame, textvariable=self.db2_password, width=20, show="*").grid(
            row=1, column=1, sticky=tk.W, padx=5)

        ttk.Label(db2_frame, text="База данных:").grid(row=1, column=2, sticky=tk.W, padx=10)
        self.db2_name = tk.StringVar(value="s12pays202504041322")
        ttk.Entry(db2_frame, textvariable=self.db2_name, width=20).grid(
            row=1, column=3, sticky=tk.W)

        # Прогресс-бар
        progress_frame = ttk.Frame(search_frame)
        progress_frame.pack(fill=tk.X, pady=10)

        self.search_progress_label = ttk.Label(progress_frame, text="Готов к поиску")
        self.search_progress_label.pack(side=tk.TOP, fill=tk.X)

        self.search_progress = ttk.Progressbar(
            progress_frame, orient=tk.HORIZONTAL, mode='determinate')
        self.search_progress.pack(fill=tk.X)

        # Кнопки управления
        btn_frame = ttk.Frame(search_frame)
        btn_frame.pack(fill=tk.X, pady=10)

        self.search_btn = ttk.Button(
            btn_frame, text="Найти пациента",
            command=self.start_patient_search)
        self.search_btn.pack(side=tk.LEFT, padx=5)

        self.stop_search_btn = ttk.Button(
            btn_frame, text="Остановить поиск",
            command=self.stop_patient_search,
            state=tk.DISABLED)
        self.stop_search_btn.pack(side=tk.LEFT, padx=5)

    def setup_common_elements(self):
        """Общие элементы для всех вкладок"""
        # Статус бар
        self.status = tk.StringVar(value="Готов к работе")
        status_bar = ttk.Label(self.root, textvariable=self.status, relief=tk.SUNKEN)
        status_bar.pack(fill=tk.X, pady=5)

        # Лог
        log_frame = ttk.LabelFrame(self.root, text="Лог выполнения", padding=10)
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(log_frame, height=10, state=tk.DISABLED)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(log_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.log_text.yview)

    def log_message(self, message):
        """Вывод сообщения в лог"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        logging.info(message)

    def start_patient_search(self):
        """Запуск поиска пациента"""
        last_name = self.patient_lastname.get().strip()
        first_name = self.patient_firstname.get().strip()
        patr_name = self.patient_patrname.get().strip()

        if not last_name and not first_name and not patr_name:
            messagebox.showerror("Ошибка", "Введите ФИО пациента!")
            return

        # Параметры для первой базы
        db1_params = {
            'host': self.db1_host.get(),
            'user': self.db1_user.get(),
            'password': self.db1_password.get(),
            'database': self.db1_name.get(),
            'patient_lastname': last_name,
            'patient_firstname': first_name,
            'patient_patrname': patr_name
        }

        # Параметры для второй базы
        db2_params = {
            'host': self.db2_host.get(),
            'user': self.db2_user.get(),
            'password': self.db2_password.get(),
            'database': self.db2_name.get(),
            'patient_lastname': last_name,
            'patient_firstname': first_name,
            'patient_patrname': patr_name
        }

        # Настройка интерфейса перед поиском
        self.processing = True
        self.search_btn.config(state=tk.DISABLED)
        self.stop_search_btn.config(state=tk.NORMAL)
        self.status.set(f"Поиск пациента: {last_name} {first_name} {patr_name}")
        self.log_message(f"Начало поиска пациента: {last_name} {first_name} {patr_name}")
        self.search_progress['value'] = 0

        # Запуск поиска в отдельном потоке
        Thread(
            target=self.run_patient_search,
            args=(db1_params, db2_params),
            daemon=True
        ).start()

    def stop_patient_search(self):
        """Остановка поиска пациента"""
        self.patient_searcher.stop_search = True
        self.status.set("Поиск прерван")
        self.log_message("Поиск пациента прерван пользователем")
        self.search_btn.config(state=tk.NORMAL)
        self.stop_search_btn.config(state=tk.DISABLED)
        self.processing = False

    def run_patient_search(self, db1_params, db2_params):
        """Выполнение поиска пациента"""
        try:
            # Поиск в первой базе
            self.update_search_progress(0, 2)
            self.log_message(f"Поиск в базе {db1_params['database']}...")
            df1 = self.patient_searcher.search_patient(db1_params)

            # Поиск во второй базе
            self.update_search_progress(1, 2)
            self.log_message(f"Поиск в базе {db2_params['database']}...")
            df2 = self.patient_searcher.search_patient(db2_params)

            # Сохранение результатов
            self.update_search_progress(2, 2)
            result_file_name = f'{db1_params["patient_lastname"]}{("_" + db1_params["patient_firstname"][0].upper()) if db1_params["patient_firstname"] else ""}{"_" + db1_params["patient_patrname"][0].upper() if db1_params["patient_patrname"] else ""}_results.xlsx'
            output_file = os.path.join(
                'results',
                result_file_name
            )

            result = self.patient_searcher.save_results(df1, df2, output_file)

            if result:
                self.log_message(f"Результаты сохранены в: {output_file}")
                messagebox.showinfo(
                    "Готово",
                    f"Результаты поиска сохранены в:\n{output_file}"
                )
            else:
                messagebox.showwarning(
                    "Внимание",
                    "Пациент не найден в указанных базах данных"
                )

        except Exception as e:
            self.log_message(f"Ошибка поиска: {str(e)}")
            messagebox.showerror("Ошибка", f"Ошибка при поиске пациента:\n{str(e)}")
        finally:
            self.processing = False
            self.search_btn.config(state=tk.NORMAL)
            self.stop_search_btn.config(state=tk.DISABLED)
            self.status.set("Поиск завершен")

    def update_search_progress(self, current: int, total: int):
        """Обновление прогресс-бара поиска"""
        if total > 0:
            percent = (current / total) * 100
            self.search_progress['value'] = percent
            self.search_progress_label.config(
                text=f"Выполнено: {current}/{total} ({percent:.1f}%)")
        self.root.update_idletasks()


if __name__ == "__main__":
    root = tk.Tk()
    app = DBFMergerApp(root)
    root.mainloop()