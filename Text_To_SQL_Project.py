import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import json
import os
from datetime import datetime
from collections import Counter
import snowflake.connector
from configparser import ConfigParser
import pandas as pd

class SnowflakeDatabaseConnector:
    def __init__(self, config_file='snowflake_config.ini'):
        self.config_file = config_file
        self.connection = None
        self.config = self.load_config()
        
    def load_config(self):
        """Load Snowflake configuration from file or create default"""
        config = ConfigParser()
        
        if os.path.exists(self.config_file):
            config.read(self.config_file)
        else:
            # Create default configuration template
            config['SNOWFLAKE'] = {
                'user': 'YOUR_USERNAME',
                'password': 'YOUR_PASSWORD',
                'account': 'your-account-locator',
                'warehouse': 'YOUR_WAREHOUSE',
                'database': 'YOUR_DATABASE',
                'schema': 'YOUR_SCHEMA',
                'role': 'YOUR_ROLE',
                'table': 'YOUR_TABLE'
            }
            
            # Save default config
            with open(self.config_file, 'w') as f:
                config.write(f)
            
            print(f"Created default config file: {self.config_file}")
            print("Please update the configuration with your Snowflake credentials.")
            
        return config
    
    def test_connection(self):
        """Test Snowflake connection"""
        try:
            conn = snowflake.connector.connect(
                user=self.config['SNOWFLAKE']['user'],
                password=self.config['SNOWFLAKE']['password'],
                account=self.config['SNOWFLAKE']['account'],
                warehouse=self.config['SNOWFLAKE']['warehouse'],
                database=self.config['SNOWFLAKE']['database'],
                schema=self.config['SNOWFLAKE']['schema'],
                role=self.config['SNOWFLAKE']['role']
            )
            conn.close()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
    
    def get_connection(self):
        """Get Snowflake connection"""
        if not self.connection or self.connection.is_closed():
            try:
                self.connection = snowflake.connector.connect(
                    user=self.config['SNOWFLAKE']['user'],
                    password=self.config['SNOWFLAKE']['password'],
                    account=self.config['SNOWFLAKE']['account'],
                    warehouse=self.config['SNOWFLAKE']['warehouse'],
                    database=self.config['SNOWFLAKE']['database'],
                    schema=self.config['SNOWFLAKE']['schema'],
                    role=self.config['SNOWFLAKE']['role']
                )
            except Exception as e:
                raise Exception(f"Failed to connect to Snowflake: {str(e)}")
        
        return self.connection
    
    def execute_query(self, query, limit=100):
        """Execute query and return results as DataFrame"""
        try:
            conn = self.get_connection()
            
            # Add limit to prevent excessive results
            if 'LIMIT' not in query.upper():
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, conn)
            return df
            
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def get_subscriber_data(self, filters=None):
        """Get subscriber data with optional filters"""
        # Build fully qualified table name from config
        database = self.config['SNOWFLAKE']['database']
        schema = self.config['SNOWFLAKE']['schema']
        table = self.config['SNOWFLAKE']['table']
        full_table_name = f"{database}.{schema}.{table}"
        
        base_query = f"""
            SELECT 
                DETOK_SBSCRBR_ID as id,
                CONCAT(DETOK_FRST_NM, ' ', DETOK_LAST_NM) as name,
                MCID as email,
                'Active' as status,
                PROD_CF_NM as plan,
                MDCL_MCID_EFCTV_DT as join_date,
                MDCL_MCID_TRMNTN_DT as last_active_date,
                MDCL_RVNU_PREM_AMT as monthly_fee,
                ST_CD as state
            FROM {full_table_name}
            WHERE 1=1
        """
        
        # Apply filters
        if filters:
            if 'status' in filters:
                if filters['status'] == 'Active':
                    base_query += " AND MDCL_MCID_TRMNTN_DT IS NULL"
                elif filters['status'] == 'Inactive':
                    base_query += " AND MDCL_MCID_TRMNTN_DT IS NOT NULL"
            if 'plan' in filters:
                base_query += f" AND PROD_CF_NM ILIKE '%{filters['plan']}%'"
            if 'state' in filters:
                base_query += f" AND ST_CD = '{filters['state']}'"
            if 'search_term' in filters:
                base_query += f" AND (CONCAT(DETOK_FRST_NM, ' ', DETOK_LAST_NM) ILIKE '%{filters['search_term']}%' OR MCID ILIKE '%{filters['search_term']}%')"
        
        base_query += " ORDER BY MDCL_MCID_EFCTV_DT DESC"
        
        return self.execute_query(base_query)
    
    def get_subscriber_stats(self):
        """Get subscriber statistics"""
        # Build fully qualified table name from config
        database = self.config['SNOWFLAKE']['database']
        schema = self.config['SNOWFLAKE']['schema']
        table = self.config['SNOWFLAKE']['table']
        full_table_name = f"{database}.{schema}.{table}"
        
        stats_query = f"""
            SELECT 
                COUNT(DISTINCT DETOK_SBSCRBR_ID) as total_subscribers,
                COUNT(DISTINCT CASE WHEN MDCL_MCID_TRMNTN_DT IS NULL THEN DETOK_SBSCRBR_ID END) as active_subscribers,
                COUNT(DISTINCT CASE WHEN MDCL_MCID_TRMNTN_DT IS NOT NULL THEN DETOK_SBSCRBR_ID END) as inactive_subscribers,
                COUNT(DISTINCT CASE WHEN PROD_CF_NM LIKE '%Premium%' THEN DETOK_SBSCRBR_ID END) as premium_members,
                COUNT(DISTINCT CASE WHEN PROD_CF_NM LIKE '%Basic%' THEN DETOK_SBSCRBR_ID END) as basic_members,
                COUNT(DISTINCT CASE WHEN PROD_CF_NM LIKE '%Enterprise%' THEN DETOK_SBSCRBR_ID END) as enterprise_members,
                AVG(MDCL_RVNU_PREM_AMT) as avg_monthly_fee,
                MAX(MDCL_MCID_EFCTV_DT) as latest_join_date
            FROM {full_table_name}
        """
        
        return self.execute_query(stats_query)
    
    def close_connection(self):
        """Close Snowflake connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()

class SubscriberQueryUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Subscriber Information Query System")
        self.root.geometry("1400x900")
        self.root.configure(bg='#ffffff')
        
        # Initialize Snowflake connector
        try:
            self.db_connector = SnowflakeDatabaseConnector()
            # Test the connection immediately
            success, message = self.db_connector.test_connection()
            if success:
                self.connection_status = "Connected"
                db_name = self.db_connector.config['SNOWFLAKE']['database']
                table_name = self.db_connector.config['SNOWFLAKE']['table']
                messagebox.showinfo("Database Connection", f"✅ Successfully connected to Snowflake!\n\nDatabase: {db_name}\nTable: {table_name}")
            else:
                self.db_connector = None
                self.connection_status = f"Connection Error: {message}"
                messagebox.showerror("Database Connection", f"❌ Failed to connect to Snowflake:\n{message}\n\nPlease check your credentials in snowflake_config.ini")
        except Exception as e:
            self.db_connector = None
            self.connection_status = f"Connection Error: {str(e)}"
            messagebox.showerror("Database Connection", f"❌ Failed to initialize Snowflake connector:\n{str(e)}\n\nPlease check your connection settings.")
        
        # Initialize query history
        self.history_file = "subscriber_query_history.json"
        self.query_history = self.load_query_history()
        
        # Setup UI
        self.setup_styles()
        self.create_widgets()
        self.update_recent_queries()
        self.update_top_searches()
        self.update_statistics()
        
    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure dark theme colors
        bg_primary = '#1a1a2e'      # Deep blue-black
        bg_secondary = '#16213e'    # Dark blue
        bg_tertiary = '#0f3460'     # Medium blue
        accent_cyan = '#00d9ff'     # Bright cyan
        accent_purple = '#7b2cbf'   # Purple
        accent_pink = '#f72585'     # Pink
        accent_green = '#06ffa5'    # Neon green
        accent_orange = '#ff9500'   # Orange
        text_primary = '#ffffff'    # White
        text_secondary = '#b8c5d6'  # Light gray
        text_accent = '#00d9ff'     # Cyan accent
        
        # Configure custom styles with dark theme
        style.configure('Title.TLabel', 
                       font=('Segoe UI', 20, 'bold'), 
                       background=bg_primary, 
                       foreground=accent_cyan)
        
        style.configure('Header.TLabel', 
                       font=('Segoe UI', 12, 'bold'), 
                       background=bg_secondary, 
                       foreground=text_primary)
        
        style.configure('Action.TButton', 
                       font=('Segoe UI', 10, 'bold'), 
                       padding=(12, 8),
                       background=bg_tertiary,
                       foreground=text_primary,
                       borderwidth=0,
                       focuscolor='none')
        
        style.map('Action.TButton',
                 background=[('active', accent_purple), ('pressed', accent_pink)],
                 foreground=[('active', text_primary), ('pressed', text_primary)])
        
        style.configure('Info.TLabel', 
                       font=('Segoe UI', 9), 
                       background=bg_primary, 
                       foreground=text_secondary)
        
        style.configure('Dark.TLabelframe', 
                       background=bg_secondary,
                       foreground=accent_cyan,
                       borderwidth=2,
                       relief='solid')
        
        style.configure('Dark.TLabelframe.Label', 
                       background=bg_secondary,
                       foreground=accent_purple,
                       font=('Segoe UI', 11, 'bold'))
        
        style.configure('Dark.TEntry', 
                       fieldbackground=bg_tertiary,
                       foreground=text_primary,
                       borderwidth=2,
                       relief='solid',
                       insertcolor=accent_cyan)
        
        style.configure('Dark.TSeparator', 
                       background=accent_pink,
                       relief='solid')
        
        # Store colors for use
        self.colors = {
            'bg_primary': bg_primary,
            'bg_secondary': bg_secondary,
            'bg_tertiary': bg_tertiary,
            'accent_cyan': accent_cyan,
            'accent_purple': accent_purple,
            'accent_pink': accent_pink,
            'accent_green': accent_green,
            'accent_orange': accent_orange,
            'text_primary': text_primary,
            'text_secondary': text_secondary,
            'text_accent': text_accent
        }
        
    def create_widgets(self):
        # Main container with padding
        main_container = ttk.Frame(self.root, padding="20", style='Dark.TLabelframe')
        main_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_container.columnconfigure(1, weight=1)
        main_container.rowconfigure(2, weight=1)
        
        # Header Section
        header_frame = ttk.Frame(main_container)
        header_frame.grid(row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 20))
        
        title_label = ttk.Label(header_frame, 
                               text="🔍 Subscriber Information Query System", 
                               style='Title.TLabel')
        title_label.pack()
        
        subtitle_label = ttk.Label(header_frame, 
                                  text="Enter subscriber queries to search and retrieve information", 
                                  style='Info.TLabel')
        subtitle_label.pack(pady=(5, 0))
        
        # Left Panel - Query Input & History
        left_frame = ttk.LabelFrame(main_container, 
                                   text="Query Input & History", 
                                   padding="15",
                                   style='Dark.TLabelframe')
        left_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(0, 15))
        
        # Query Input Section
        ttk.Label(left_frame, 
                 text="📝 Enter Subscriber Query", 
                 style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, pady=(0, 10))
        
        # Query input field with frame
        query_input_frame = ttk.Frame(left_frame)
        query_input_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        query_input_frame.columnconfigure(0, weight=1)
        
        self.query_var = tk.StringVar()
        self.query_entry = ttk.Entry(query_input_frame, 
                                    textvariable=self.query_var, 
                                    width=45, 
                                    font=('Segoe UI', 11))
        self.query_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 5))
        self.query_entry.bind('<Return>', lambda e: self.execute_query())
        
        # Action buttons
        button_frame = ttk.Frame(query_input_frame)
        button_frame.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        ttk.Button(button_frame, 
                  text="🔍 Search", 
                  command=self.execute_query, 
                  style='Action.TButton').pack(side=tk.LEFT, padx=(0, 8))
        
        ttk.Button(button_frame, 
                  text="🗑️ Clear", 
                  command=self.clear_query, 
                  style='Action.TButton').pack(side=tk.LEFT, padx=(0, 8))
        
        ttk.Button(button_frame, 
                  text="📊 Export History", 
                  command=self.export_history, 
                  style='Action.TButton').pack(side=tk.LEFT)
        
        # Recent Queries Section
        ttk.Separator(left_frame, orient='horizontal').grid(row=2, column=0, sticky=(tk.W, tk.E), pady=15)
        
        ttk.Label(left_frame, 
                 text="🕐 Recent Queries", 
                 style='Header.TLabel').grid(row=3, column=0, sticky=tk.W, pady=(0, 8))
        
        recent_frame = ttk.Frame(left_frame)
        recent_frame.grid(row=4, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        recent_frame.columnconfigure(0, weight=1)
        
        self.recent_listbox = tk.Listbox(recent_frame, 
                                        height=8, 
                                        font=('Segoe UI', 9),
                                        bg=self.colors['bg_tertiary'],
                                        fg=self.colors['text_primary'],
                                        selectbackground=self.colors['accent_cyan'],
                                        selectforeground=self.colors['bg_primary'],
                                        relief='solid',
                                        borderwidth=2,
                                        activestyle='none')
        self.recent_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.recent_listbox.bind('<<ListboxSelect>>', self.on_recent_select)
        self.recent_listbox.bind('<Double-Button-1>', lambda e: self.execute_selected_query())
        
        # Scrollbar for recent queries
        recent_scrollbar = ttk.Scrollbar(recent_frame, orient="vertical", command=self.recent_listbox.yview)
        recent_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.recent_listbox.configure(yscrollcommand=recent_scrollbar.set)
        
        # Try These Queries Section
        ttk.Label(left_frame, 
                 text="💡 Try These Queries", 
                 style='Header.TLabel').grid(row=5, column=0, sticky=tk.W, pady=(0, 8))
        
        try_frame = ttk.Frame(left_frame)
        try_frame.grid(row=6, column=0, sticky=(tk.W, tk.E), pady=(0, 15))
        try_frame.columnconfigure(0, weight=1)
        
        # Example queries with descriptions
        example_queries = [
            ("subscriber", "Shows all subscribers from FACT_ISG"),
            ("active subscriber", "Filters for active members (no termination date)"),
            ("inactive subscriber", "Shows terminated members"),
            ("california subscriber", "Filters by California state"),
            ("texas subscriber", "Filters by Texas state")
        ]
        
        self.try_listbox = tk.Listbox(try_frame, 
                                     height=5, 
                                     font=('Segoe UI', 9),
                                     bg=self.colors['bg_secondary'],
                                     fg=self.colors['text_primary'],
                                     selectbackground=self.colors['accent_green'],
                                     selectforeground=self.colors['bg_primary'],
                                     relief='solid',
                                     borderwidth=2,
                                     activestyle='none')
        self.try_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Add example queries with descriptions
        for query, description in example_queries:
            self.try_listbox.insert(tk.END, f"{query} - {description}")
        
        # Scrollbar for try queries
        try_scrollbar = ttk.Scrollbar(try_frame, orient="vertical", command=self.try_listbox.yview)
        try_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.try_listbox.configure(yscrollcommand=try_scrollbar.set)
        
        # Bind events
        self.try_listbox.bind('<<ListboxSelect>>', self.on_try_select)
        self.try_listbox.bind('<Double-Button-1>', lambda e: self.execute_try_query())
        
        # Top Searches Section
        ttk.Label(left_frame, 
                 text="🔥 Top Searches", 
                 style='Header.TLabel').grid(row=7, column=0, sticky=tk.W, pady=(0, 8))
        
        top_frame = ttk.Frame(left_frame)
        top_frame.grid(row=8, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        top_frame.columnconfigure(0, weight=1)
        
        self.top_listbox = tk.Listbox(top_frame, 
                                     height=8, 
                                     font=('Segoe UI', 9),
                                     bg='white',
                                     selectbackground='#e74c3c',
                                     selectforeground='white')
        self.top_listbox.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.top_listbox.bind('<<ListboxSelect>>', self.on_top_select)
        self.top_listbox.bind('<Double-Button-1>', lambda e: self.execute_selected_query())
        
        # Scrollbar for top searches
        top_scrollbar = ttk.Scrollbar(top_frame, orient="vertical", command=self.top_listbox.yview)
        top_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        self.top_listbox.configure(yscrollcommand=top_scrollbar.set)
        
        # Configure left frame grid weights
        left_frame.columnconfigure(0, weight=1)
        left_frame.rowconfigure(4, weight=1)
        left_frame.rowconfigure(6, weight=1)
        
        # Middle Panel - Results Display
        middle_frame = ttk.LabelFrame(main_container, 
                                     text="Query Results", 
                                     padding="15")
        middle_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=15)
        
        # Results header with info
        results_header = ttk.Frame(middle_frame)
        results_header.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        results_header.columnconfigure(1, weight=1)
        
        ttk.Label(results_header, 
                 text="📋 Search Results", 
                 style='Header.TLabel').grid(row=0, column=0, sticky=tk.W)
        
        self.result_count_var = tk.StringVar(value="0 results")
        ttk.Label(results_header, 
                 textvariable=self.result_count_var, 
                 style='Info.TLabel').grid(row=0, column=1, sticky=tk.E)
        
        # Results display area
        self.results_text = scrolledtext.ScrolledText(middle_frame, 
                                                     width=70, height=35, 
                                                     font=('Consolas', 10), 
                                                     wrap=tk.WORD,
                                                     bg='white',
                                                     fg='#2c3e50',
                                                     selectbackground='#3498db')
        self.results_text.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Status bar
        status_frame = ttk.Frame(middle_frame)
        status_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(10, 0))
        status_frame.columnconfigure(0, weight=1)
        
        self.status_var = tk.StringVar(value="✅ Ready to search")
        status_bar = ttk.Label(status_frame, 
                              textvariable=self.status_var, 
                              relief=tk.SUNKEN, 
                              anchor=tk.W,
                              padding=(5, 3))
        status_bar.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        # Configure middle frame grid weights
        middle_frame.columnconfigure(0, weight=1)
        middle_frame.rowconfigure(1, weight=1)
        
        # Right Panel - Statistics & Management
        right_frame = ttk.LabelFrame(main_container, 
                                    text="Statistics & Management", 
                                    padding="15")
        right_frame.grid(row=1, column=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(15, 0))
        
        # Statistics Section
        ttk.Label(right_frame, 
                 text="📈 Query Statistics", 
                 style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, pady=(0, 10))
        
        stats_container = ttk.Frame(right_frame)
        stats_container.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 15))
        stats_container.columnconfigure(0, weight=1)
        
        self.stats_text = tk.Text(stats_container, 
                                 width=30, height=12, 
                                 font=('Segoe UI', 9), 
                                 wrap=tk.WORD,
                                 bg='#ecf0f1',
                                 fg='#2c3e50',
                                 relief=tk.FLAT,
                                 padx=10, pady=10)
        self.stats_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.stats_text.config(state=tk.DISABLED)
        
        # Management buttons
        ttk.Separator(right_frame, orient='horizontal').grid(row=2, column=0, sticky=(tk.W, tk.E), pady=15)
        
        management_frame = ttk.Frame(right_frame)
        management_frame.grid(row=3, column=0, sticky=(tk.W, tk.E))
        management_frame.columnconfigure(0, weight=1)
        
        ttk.Button(management_frame, 
                  text="🗑️ Clear History", 
                  command=self.clear_history, 
                  style='Action.TButton').grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 8))
        
        ttk.Button(management_frame, 
                  text="🔄 Refresh Stats", 
                  command=self.refresh_statistics, 
                  style='Action.TButton').grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        # Configure right frame grid weights
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(1, weight=1)
        
    def load_query_history(self):
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r') as f:
                    return json.load(f)
            except:
                return {'queries': [], 'stats': {}}
        return {'queries': [], 'stats': {}}
    
    def save_query_history(self):
        with open(self.history_file, 'w') as f:
            json.dump(self.query_history, f, indent=2)
    
    def add_query_to_history(self, query):
        timestamp = datetime.now().isoformat()
        self.query_history['queries'].append({
            'query': query,
            'timestamp': timestamp
        })
        
        # Keep only last 100 queries
        if len(self.query_history['queries']) > 100:
            self.query_history['queries'] = self.query_history['queries'][-100:]
        
        # Update stats
        if query in self.query_history['stats']:
            self.query_history['stats'][query] += 1
        else:
            self.query_history['stats'][query] = 1
        
        self.save_query_history()
    
    def update_recent_queries(self):
        self.recent_listbox.delete(0, tk.END)
        recent_queries = self.query_history['queries'][-10:][::-1]  # Last 10, reversed
        for item in recent_queries:
            self.recent_listbox.insert(tk.END, item['query'])
    
    def update_top_searches(self):
        self.top_listbox.delete(0, tk.END)
        stats = self.query_history['stats']
        if stats:
            top_queries = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:10]
            for query, count in top_queries:
                self.top_listbox.insert(tk.END, f"{query} ({count})")
    
    def update_statistics(self):
        self.stats_text.config(state=tk.NORMAL)
        self.stats_text.delete(1.0, tk.END)
        
        total_queries = len(self.query_history['queries'])
        unique_queries = len(self.query_history['stats'])
        
        stats_text = f"📊 Total Queries: {total_queries}\n"
        stats_text += f"🎯 Unique Queries: {unique_queries}\n\n"
        
        if self.query_history['queries']:
            last_query = self.query_history['queries'][-1]
            last_time = datetime.fromisoformat(last_query['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
            stats_text += f"🕐 Last Query:\n{last_time}\n\n"
        
        if self.query_history['stats']:
            most_common = max(self.query_history['stats'].items(), key=lambda x: x[1])
            stats_text += f"🔥 Most Common:\n{most_common[0]}\n({most_common[1]} times)"
        
        self.stats_text.insert(1.0, stats_text)
        self.stats_text.config(state=tk.DISABLED)
    
    def refresh_statistics(self):
        self.update_statistics()
        self.status_var.set("📊 Statistics refreshed")
    
    def execute_subscriber_query(self, query):
        """Execute subscriber query using Snowflake database"""
        
        if not self.db_connector:
            raise Exception("Database connector not initialized")
        
        # Parse query and build filters
        filters = self.parse_query_to_filters(query)
        
        # Get data from database
        df = self.db_connector.get_subscriber_data(filters)
        
        if not df.empty:
            return self.format_dataframe_results(df, query)
        else:
            return "📋 No subscribers found matching your criteria."
    
    def parse_query_to_filters(self, query):
        """Parse natural language query into database filters"""
        filters = {}
        query_lower = query.lower()
        
        # Status filters
        if "active" in query_lower:
            filters['status'] = 'Active'
        elif "inactive" in query_lower:
            filters['status'] = 'Inactive'
        elif "suspended" in query_lower:
            filters['status'] = 'Suspended'
        
        # Plan filters
        if "premium" in query_lower:
            filters['plan'] = 'Premium'
        elif "basic" in query_lower:
            filters['plan'] = 'Basic'
        elif "enterprise" in query_lower:
            filters['plan'] = 'Enterprise'
        
        # State filters (US state codes)
        states = {'california': 'CA', 'texas': 'TX', 'florida': 'FL', 'new york': 'NY', 
                  'illinois': 'IL', 'pennsylvania': 'PA', 'ohio': 'OH', 'georgia': 'GA',
                  'north carolina': 'NC', 'michigan': 'MI'}
        for state_name, state_code in states.items():
            if state_name in query_lower:
                filters['state'] = state_code
                break
        
        # Search term for name/email
        if any(keyword in query_lower for keyword in ["subscriber", "member", "customer"]):
            # Extract potential search terms
            words = query_lower.split()
            for word in words:
                if len(word) > 2 and word not in ['subscriber', 'member', 'customer', 'active', 'inactive', 'premium', 'basic', 'enterprise']:
                    filters['search_term'] = word
                    break
        
        return filters
    
    def format_dataframe_results(self, df, query):
        """Format pandas DataFrame results for display"""
        if df.empty:
            return "📋 No subscribers found matching your criteria."
        
        formatted_results = f"👥 Subscriber Information\n🔍 Query: {query}\n📊 Found: {len(df)} results\n\n"
        formatted_results += "=" * 60 + "\n\n"
        
        for i, (_, row) in enumerate(df.iterrows(), 1):
            formatted_results += f"📋 Record {i}:\n"
            formatted_results += f"  🆔 ID: {row.get('id', 'N/A')}\n"
            formatted_results += f"  👤 Name: {row.get('name', 'N/A')}\n"
            formatted_results += f"  📧 Email: {row.get('email', 'N/A')}\n"
            formatted_results += f"  ✅ Status: {row.get('status', 'N/A')}\n"
            formatted_results += f"  💼 Plan: {row.get('plan', 'N/A')}\n"
            
            if 'join_date' in row and pd.notna(row['join_date']):
                formatted_results += f"  📅 Join Date: {row['join_date']}\n"
            
            if 'last_active_date' in row and pd.notna(row['last_active_date']):
                formatted_results += f"  🕐 Last Active: {row['last_active_date']}\n"
            
            if 'monthly_fee' in row and pd.notna(row['monthly_fee']):
                formatted_results += f"  💰 Monthly Fee: ${row['monthly_fee']:.2f}\n"
            
            if 'state' in row and pd.notna(row['state']):
                formatted_results += f"  🌍 State: {row['state']}\n"
            
            formatted_results += "-" * 50 + "\n\n"
        
        return formatted_results
    
    def execute_query(self):
        query = self.query_var.get().strip()
        if not query:
            messagebox.showwarning("⚠️ Warning", "Please enter a query to search")
            return
        
        self.status_var.set("🔍 Searching...")
        self.results_text.delete(1.0, tk.END)
        
        try:
            # Execute subscriber query from Snowflake database
            result = self.execute_subscriber_query(query)
            
            # Display results
            table_name = self.db_connector.config['SNOWFLAKE']['table']
            self.results_text.insert(tk.END, f"🔍 Query: {query}\n")
            self.results_text.insert(tk.END, f"🕐 Executed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            self.results_text.insert(tk.END, f"🔗 Data Source: Snowflake Database ({table_name})\n")
            self.results_text.insert(tk.END, "=" * 60 + "\n\n")
            self.results_text.insert(tk.END, result)
            
            # Update result count
            result_lines = result.strip().split('\n')
            result_count = len([line for line in result_lines if line.strip() and not line.startswith('-') and not line.startswith('🔍') and not line.startswith('🕐') and not line.startswith('🔗')])
            self.result_count_var.set(f"{result_count} items found")
            
            # Add to history
            self.add_query_to_history(query)
            self.update_recent_queries()
            self.update_top_searches()
            self.update_statistics()
            
            self.status_var.set(f"✅ Query completed - {result_count} results found")
            
        except Exception as e:
            self.results_text.insert(tk.END, f"❌ Error executing query: {str(e)}")
            self.status_var.set("❌ Query failed")
            messagebox.showerror("❌ Error", f"Failed to execute query: {str(e)}")
            
    def clear_query(self):
        self.query_var.set("")
        self.query_entry.focus()
        self.status_var.set("🗑️ Query cleared")
    
    def execute_selected_query(self):
        selected = None
        if self.recent_listbox.curselection():
            selected = self.recent_listbox.get(self.recent_listbox.curselection())
        elif self.top_listbox.curselection():
            selected = self.top_listbox.get(self.top_listbox.curselection())
            selected = selected.split(' (')[0]  # Remove count from top searches
        
        if selected:
            self.query_var.set(selected)
            self.execute_query()
    
    def on_recent_select(self, event):
        if self.recent_listbox.curselection():
            selected = self.recent_listbox.get(self.recent_listbox.curselection())
            self.query_var.set(selected)
    
    def on_top_select(self, event):
        if self.top_listbox.curselection():
            selected = self.top_listbox.get(self.top_listbox.curselection())
            selected = selected.split(' (')[0]  # Remove count
            self.query_var.set(selected)
    
    def on_try_select(self, event):
        if self.try_listbox.curselection():
            selected = self.try_listbox.get(self.try_listbox.curselection())
            # Extract just the query part (before " - ")
            query = selected.split(' - ')[0]
            self.query_var.set(query)
    
    def execute_try_query(self):
        if self.try_listbox.curselection():
            selected = self.try_listbox.get(self.try_listbox.curselection())
            # Extract just the query part (before " - ")
            query = selected.split(' - ')[0]
            self.query_var.set(query)
            self.execute_query()
    
    def clear_history(self):
        if messagebox.askyesno("⚠️ Confirm", "Clear all query history? This action cannot be undone."):
            self.query_history = {'queries': [], 'stats': {}}
            self.save_query_history()
            self.update_recent_queries()
            self.update_top_searches()
            self.update_statistics()
            self.status_var.set("🗑️ Query history cleared")
            messagebox.showinfo("✅ Success", "Query history has been cleared successfully.")
    
    def export_history(self):
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            export_file = f"subscriber_query_history_{timestamp}.json"
            with open(export_file, 'w') as f:
                json.dump(self.query_history, f, indent=2)
            self.status_var.set(f"📊 History exported to {export_file}")
            messagebox.showinfo("✅ Success", f"Query history exported to:\n{export_file}")
        except Exception as e:
            messagebox.showerror("❌ Error", f"Failed to export history: {str(e)}")

def main():
    root = tk.Tk()
    app = SubscriberQueryUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()