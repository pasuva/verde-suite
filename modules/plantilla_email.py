import warnings
warnings.filterwarnings("ignore", category=UserWarning)

# -*- coding: utf-8 -*-
def generar_html(asunto, contenido):
    """
    Genera un correo en formato HTML con un diseño elegante.
    :param asunto: Título del correo.
    :param contenido: Diccionario con los datos a mostrar en el cuerpo.
    :return: Cadena con el HTML formateado.
    """
    html_template = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{asunto}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f4f4f4;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 600px;
                background: #ffffff;
                margin: 20px auto;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
            }}
            h2 {{
                color: #333;
                text-align: center;
            }}
            .details {{
                background: #f9f9f9;
                padding: 15px;
                border-radius: 5px;
                margin-top: 10px;
            }}
            .details p {{
                margin: 5px 0;
            }}
            .highlight {{
                color: #007bff;
                font-weight: bold;
            }}
            .footer {{
                margin-top: 20px;
                text-align: center;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>{asunto}</h2>
            <p>Estimado Usuario,</p>
            <p>{contenido.get("mensaje", "Información relevante a continuación:")}</p>

            <div class="details">
                {''.join(f'<p><strong>{k}:</strong> {v}</p>' for k, v in contenido.items() if k != "mensaje")}
            </div>

            <p>Saludos cordiales,</p>
            <p><strong>Equipo de IT y Sistemas</strong></p>

            <div class="footer">
                <p>📧 Este es un correo automático, por favor no respondas a este mensaje.</p>
                <img src="https://verdetuoperador.com/wp-content/uploads/2024/11/logotipo_verde_tuoperador_telefonia_fibra_y_television.png"/>
            </div>
        </div>
    </body>
    </html>
    """
    return html_template
