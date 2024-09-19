import psycopg2
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="220500532001",
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def fetch_reviews(product_asin):
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT customer_id, rating, votes, helpful, date
    FROM Reviews
    JOIN Products ON Products.id = Reviews.product_id
    WHERE Products.asin = %s
    ORDER BY helpful DESC, rating DESC
    LIMIT 5
    '''
    cursor = conn.cursor()
    cursor.execute(query, (product_asin,))
    top_reviews = cursor.fetchall()

    query = '''
    SELECT customer_id, rating, votes, helpful, date
    FROM Reviews
    JOIN Products ON Products.id = Reviews.product_id
    WHERE Products.asin = %s
    ORDER BY helpful DESC, rating ASC
    LIMIT 5
    '''
    cursor.execute(query, (product_asin,))
    bottom_reviews = cursor.fetchall()

    conn.close()

    return top_reviews, bottom_reviews


def fetch_similar_products_with_higher_sales(product_asin):
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT sp.similar_asin, p2.title, p2.salesrank
    FROM SimilarProducts sp
    JOIN Products p1 ON sp.asin = p1.asin
    JOIN Products p2 ON sp.similar_asin = p2.asin
    WHERE p1.asin = %s AND p2.salesrank < p1.salesrank
    ORDER BY p2.salesrank ASC
    '''

    cursor = conn.cursor()
    cursor.execute(query, (product_asin,))
    similar_products = cursor.fetchall()

    conn.close()

    return similar_products


def fetch_rating_evolution(product_asin):
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT date, AVG(rating) AS avg_rating
    FROM Reviews
    JOIN Products ON Products.id = Reviews.product_id
    WHERE Products.asin = %s
    GROUP BY date
    ORDER BY date ASC
    '''
    cursor = conn.cursor()
    cursor.execute(query, (product_asin,))
    rating_evolution = cursor.fetchall()

    conn.close()

    return rating_evolution


def fetch_top_selling_products():
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT productGroup, asin, title, salesrank
    FROM (
        SELECT productGroup, asin, title, salesrank,
            ROW_NUMBER() OVER (PARTITION BY productGroup ORDER BY salesrank ASC) AS rank
        FROM Products
    ) ranked_products
    WHERE rank <= 10;
    '''

    cursor = conn.cursor()
    cursor.execute(query)
    top_selling_products = cursor.fetchall()

    conn.close()

    return top_selling_products


def fetch_top_helpful_reviews():
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT Products.asin, Products.title, AVG(Reviews.helpful) AS avg_helpful
    FROM Reviews
    JOIN Products ON Products.id = Reviews.product_id
    GROUP BY Products.asin, Products.title
    HAVING AVG(Reviews.helpful) IS NOT NULL
    ORDER BY avg_helpful DESC
    LIMIT 10;
    '''

    cursor = conn.cursor()
    cursor.execute(query)
    top_helpful_reviews = cursor.fetchall()

    conn.close()

    return top_helpful_reviews

def fetch_top_categories_by_helpful_reviews():
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT Products.productgroup, AVG(Reviews.helpful) AS avg_helpful
    FROM Reviews
    JOIN Products ON Products.id = Reviews.product_id
    WHERE Reviews.helpful > 0
    GROUP BY Products.productgroup
    ORDER BY avg_helpful DESC
    LIMIT 5;
    '''
    cursor = conn.cursor()
    cursor.execute(query)
    top_categories = cursor.fetchall()

    conn.close()

    return top_categories


def fetch_top_customers_by_product_group():
    conn = get_db_connection()
    if conn is None:
        return [], []

    query = '''
    SELECT productGroup, customer_id, num_comments
    FROM (
        SELECT Products.productGroup, Reviews.customer_id, COUNT(Reviews.customer_id) AS num_comments,
            ROW_NUMBER() OVER (PARTITION BY Products.productGroup ORDER BY COUNT(Reviews.customer_id) DESC) AS rank
        FROM Reviews
        JOIN Products ON Products.id = Reviews.product_id
        GROUP BY Products.productGroup, Reviews.customer_id
    ) ranked_customers
    WHERE rank <= 10
    ORDER BY productGroup, num_comments DESC;
    '''
    cursor = conn.cursor()
    cursor.execute(query)
    top_customers = cursor.fetchall()

    conn.close()

    return top_customers

def reviews_to_dataframe(reviews):
    df = pd.DataFrame(reviews, columns=["Customer ID", "Rating", "Votes", "Helpful", "Date"])
    return df

def similar_products_to_dataframe(products):
    df = pd.DataFrame(products, columns=["ASIN", "Title", "Sales Rank"])
    return df

def rating_evolution_to_dataframe(rating_evolution):
    df = pd.DataFrame(rating_evolution, columns=["Date", "Average Rating"])
    return df

def top_selling_products_to_dataframe(products):
    df = pd.DataFrame(products, columns=["Product Group", "ASIN", "Title", "Sales Rank"])
    return df

def top_helpful_reviews_to_dataframe(products):
    df = pd.DataFrame(products, columns=["ASIN", "Title", "Avg Helpful Reviews"])
    return df

def top_categories_to_dataframe(categories):
    df = pd.DataFrame(categories, columns=["Product Group", "Avg Helpful Reviews"])
    return df

def top_customers_to_dataframe(customers):
    df = pd.DataFrame(customers, columns=["Product Group", "Customer ID", "Number of Comments"])
    return df

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Product Dashboard"),

    html.Label("Enter Product ASIN or Title: "),
    dcc.Input(id="asin-input", type="text", placeholder="Enter product ASIN", debounce=True),

    html.Div([
        html.Div(id="top-reviews-output", style={'flex': '1'}),
        html.Div(id="bottom-reviews-output", style={'flex': '1'})
    ], style={'display': 'flex'}),

    html.Div(id="similar-products-output"),

    html.Div(id="rating-evolution-output"),

    html.Div(id="top-selling-products-output"),

    html.Div(id="top-helpful-reviews-output",style={'margin-bottom': '10px'}),

    html.Div([
        html.Div(id="top-categories-output", style={'flex': '1'}),
        html.Div(id="top-customers-output", style={'flex': '1'})
    ])
])

@app.callback(
    [Output("top-reviews-output", "children"),
     Output("bottom-reviews-output", "children"),
     Output("similar-products-output", "children"),
     Output("rating-evolution-output", "children"),
     Output("top-selling-products-output", "children"),
     Output("top-helpful-reviews-output", "children"),
     Output("top-categories-output", "children"),
     Output("top-customers-output", "children")],
    [Input("asin-input", "value")]
)
def update_dashboard(asin_value):
    if asin_value:
        top_reviews, bottom_reviews = fetch_reviews(asin_value)
        df_top_reviews = reviews_to_dataframe(top_reviews)
        df_bottom_reviews = reviews_to_dataframe(bottom_reviews)

        top_reviews_table = html.Div([
            html.H3("Top 5 Most Helpful Reviews (Highest Rating)"),
            html.Table([
                html.Tr([html.Th(col) for col in df_top_reviews.columns])] +
                [html.Tr([html.Td(df_top_reviews.iloc[i][col]) for col in df_top_reviews.columns]) for i in range(len(df_top_reviews))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px', 'margin-top': '10px'})

        bottom_reviews_table = html.Div([
            html.H3("Top 5 Most Helpful Reviews (Lowest Rating)"),
            html.Table([
                html.Tr([html.Th(col) for col in df_bottom_reviews.columns])] +
                [html.Tr([html.Td(df_bottom_reviews.iloc[i][col]) for col in df_bottom_reviews.columns]) for i in range(len(df_bottom_reviews))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px', 'margin-top': '10px'})

        similar_products = fetch_similar_products_with_higher_sales(asin_value)
        df_similar_products = similar_products_to_dataframe(similar_products)

        similar_products_table = html.Div([
            html.H3("Similar Products with Higher Sales"),
            html.Table([
                html.Tr([html.Th(col) for col in df_similar_products.columns])] +
                [html.Tr([html.Td(df_similar_products.iloc[i][col]) for col in df_similar_products.columns]) for i in range(len(df_similar_products))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px'})

        rating_evolution = fetch_rating_evolution(asin_value)
        df_rating_evolution = rating_evolution_to_dataframe(rating_evolution)

        fig = px.line(df_rating_evolution, x="Date", y="Average Rating", title="Rating Evolution Over Time")
        rating_evolution_graph = dcc.Graph(figure=fig)

        top_selling_products = fetch_top_selling_products()
        df_top_selling_products = top_selling_products_to_dataframe(top_selling_products)

        top_selling_products_table = html.Div([
            html.H3("Top 10 Best-Selling Products by Product Group"),
            html.Table([
                html.Tr([html.Th(col) for col in df_top_selling_products.columns])] +
                [html.Tr([html.Td(df_top_selling_products.iloc[i][col]) for col in df_top_selling_products.columns]) for i in range(len(df_top_selling_products))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px'})

        top_helpful_reviews = fetch_top_helpful_reviews()
        df_top_helpful_reviews = top_helpful_reviews_to_dataframe(top_helpful_reviews)

        top_helpful_reviews_table = html.Div([
            html.H3("Top 10 Products by Helpful Reviews"),
            html.Table([
                html.Tr([html.Th(col) for col in df_top_helpful_reviews.columns])] +
                [html.Tr([html.Td(df_top_helpful_reviews.iloc[i][col]) for col in df_top_helpful_reviews.columns]) for i in range(len(df_top_helpful_reviews))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px'})

        top_categories = fetch_top_categories_by_helpful_reviews()
        df_top_categories = top_categories_to_dataframe(top_categories)

        top_categories_table = html.Div([
            html.H3("Top 5 Product Categories by Helpful Reviews"),
            html.Table([
                html.Tr([html.Th(col) for col in df_top_categories.columns])] +
                [html.Tr([html.Td(df_top_categories.iloc[i][col]) for col in df_top_categories.columns]) for i in range(len(df_top_categories))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px'})

        top_customers = fetch_top_customers_by_product_group()
        df_top_customers = top_customers_to_dataframe(top_customers)

        top_customers_table = html.Div([
            html.H3("Top 10 Customers by Product Group (Number of Comments)"),
            html.Table([
                html.Tr([html.Th(col) for col in df_top_customers.columns])] +
                [html.Tr([html.Td(df_top_customers.iloc[i][col]) for col in df_top_customers.columns]) for i in range(len(df_top_customers))]
            )
        ], style={'border': '1px solid #ccc', 'padding': '10px'})

        return top_reviews_table, bottom_reviews_table, similar_products_table, rating_evolution_graph, top_selling_products_table, top_helpful_reviews_table, top_categories_table, top_customers_table

    return "", "", "", "", "", "", "", ""

if __name__ == "__main__":
    app.run_server(debug=True)