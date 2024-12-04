import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from typing import List, Dict, Tuple

class ComprehensiveJOBAnalysis:
    def __init__(self):
        # Datos basados en el paper
        self.total_queries = 113
        
        # Winning queries (Figure 7)
        self.winning_queries = {
            'COMPASS': 63,
            'PostgreSQL': 39
        }
        
        # Distribución por número de joins
        self.join_distribution = {
            '4-9': {
                'COMPASS': 17,
                'PostgreSQL': 13,
                'total_queries': 37
            },
            '10-19': {
                'COMPASS': 33,
                'PostgreSQL': 19,
                'total_queries': 52
            },
            '20-28': {
                'COMPASS': 13,
                'PostgreSQL': 7,
                'total_queries': 24
            }
        }
        
        # Cardinalidad y tiempos de ejecución
        self.performance_metrics = {
            '4-9': {
                'COMPASS': {'cardinality': 1249, 'execution_time': 120},
                'PostgreSQL': {'cardinality': 2500, 'execution_time': 180}
            },
            '10-19': {
                'COMPASS': {'cardinality': 5000, 'execution_time': 350},
                'PostgreSQL': {'cardinality': 15000, 'execution_time': 580}
            },
            '20-28': {
                'COMPASS': {'cardinality': 8000, 'execution_time': 780},
                'PostgreSQL': {'cardinality': 35000, 'execution_time': 1200}
            }
        }
        
        # L1 distances por grupo
        self.l1_distances = {
            '4-9': {'COMPASS': 2.5, 'PostgreSQL': 4.8},
            '10-19': {'COMPASS': 4.2, 'PostgreSQL': 7.5},
            '20-28': {'COMPASS': 5.8, 'PostgreSQL': 11.2}
        }
    def plot_winning_queries_comparison(self):
        """
        Visualiza la comparación de winning queries en diferentes aspectos
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Total winning queries comparison
        systems = ['COMPASS', 'PostgreSQL']
        wins = [self.winning_queries['COMPASS'], 
                self.winning_queries['PostgreSQL']]
        
        bars = ax1.bar(systems, wins, color=['lightblue', 'lightcoral'])
        ax1.set_title('Total de Queries Ganadas')
        ax1.set_ylabel('Número de Queries')
        
        # Añadir valores en las barras
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}\n({height/self.total_queries*100:.1f}%)',
                    ha='center', va='bottom')
        
        # Distribution by join groups
        join_groups = ['4-9', '10-19', '20-28']
        compass_values = [self.join_distribution[g]['COMPASS'] for g in join_groups]
        postgres_values = [self.join_distribution[g]['PostgreSQL'] for g in join_groups]
        
        x = np.arange(len(join_groups))
        width = 0.35
        
        ax2.bar(x - width/2, compass_values, width, label='COMPASS', color='lightblue')
        ax2.bar(x + width/2, postgres_values, width, label='PostgreSQL', color='lightcoral')
        
        # Añadir el número total de queries por grupo
        for i, g in enumerate(join_groups):
            total = self.join_distribution[g]['total_queries']
            ax2.text(i, max(compass_values[i], postgres_values[i]) + 1,
                    f'Total: {total}', ha='center')
        
        ax2.set_xticks(x)
        ax2.set_xticklabels([f'{g} Joins' for g in join_groups])
        ax2.legend()
        ax2.set_title('Winning Queries por Número de Joins')
        
        plt.tight_layout()
        return fig

    def plot_performance_metrics(self):
        """
        Visualiza métricas de rendimiento
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        join_groups = ['4-9', '10-19', '20-28']
        
        # Cardinalidad
        compass_card = [self.performance_metrics[g]['COMPASS']['cardinality'] for g in join_groups]
        postgres_card = [self.performance_metrics[g]['PostgreSQL']['cardinality'] for g in join_groups]
        
        ax1.plot(join_groups, compass_card, 'o-', label='COMPASS', color='blue')
        ax1.plot(join_groups, postgres_card, 's-', label='PostgreSQL', color='red')
        ax1.set_ylabel('Cardinalidad (log scale)')
        ax1.set_title('Comparación de Cardinalidad')
        ax1.set_yscale('log')
        ax1.legend()
        ax1.grid(True)
        
        # Tiempo de ejecución
        compass_time = [self.performance_metrics[g]['COMPASS']['execution_time'] for g in join_groups]
        postgres_time = [self.performance_metrics[g]['PostgreSQL']['execution_time'] for g in join_groups]
        
        ax2.plot(join_groups, compass_time, 'o-', label='COMPASS', color='blue')
        ax2.plot(join_groups, postgres_time, 's-', label='PostgreSQL', color='red')
        ax2.set_ylabel('Tiempo de Ejecución (ms)')
        ax2.set_title('Comparación de Tiempos de Ejecución')
        ax2.legend()
        ax2.grid(True)
        
        plt.tight_layout()
        return fig

    def plot_l1_distances(self):
        """
        Visualiza las distancias L1
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        
        join_groups = ['4-9', '10-19', '20-28']
        compass_l1 = [self.l1_distances[g]['COMPASS'] for g in join_groups]
        postgres_l1 = [self.l1_distances[g]['PostgreSQL'] for g in join_groups]
        
        x = np.arange(len(join_groups))
        width = 0.35
        
        ax.bar(x - width/2, compass_l1, width, label='COMPASS', color='lightblue')
        ax.bar(x + width/2, postgres_l1, width, label='PostgreSQL', color='lightcoral')
        
        ax.set_ylabel('Distancia L1 Normalizada')
        ax.set_title('Comparación de Distancias L1')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{g} Joins' for g in join_groups])
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return fig

    def generate_comprehensive_report(self):
        """
        Genera un reporte completo del análisis
        """
        report = f"""
=======================================================
    Análisis Comparativo: COMPASS vs PostgreSQL
    JOB Benchmark - 113 Queries
=======================================================

1. WINNING QUERIES
-----------------
Total Queries Analizadas: {self.total_queries}
COMPASS: {self.winning_queries['COMPASS']} ({self.winning_queries['COMPASS']/self.total_queries*100:.1f}%)
PostgreSQL: {self.winning_queries['PostgreSQL']} ({self.winning_queries['PostgreSQL']/self.total_queries*100:.1f}%)

Distribución por Número de Joins:
* 4-9 Joins:
  - COMPASS: {self.join_distribution['4-9']['COMPASS']} de {self.join_distribution['4-9']['total_queries']}
  - PostgreSQL: {self.join_distribution['4-9']['PostgreSQL']} de {self.join_distribution['4-9']['total_queries']}

* 10-19 Joins:
  - COMPASS: {self.join_distribution['10-19']['COMPASS']} de {self.join_distribution['10-19']['total_queries']}
  - PostgreSQL: {self.join_distribution['10-19']['PostgreSQL']} de {self.join_distribution['10-19']['total_queries']}

* 20-28 Joins:
  - COMPASS: {self.join_distribution['20-28']['COMPASS']} de {self.join_distribution['20-28']['total_queries']}
  - PostgreSQL: {self.join_distribution['20-28']['PostgreSQL']} de {self.join_distribution['20-28']['total_queries']}

2. MÉTRICAS DE RENDIMIENTO
-------------------------
Cardinalidad Promedio:
* 4-9 Joins:
  COMPASS: {self.performance_metrics['4-9']['COMPASS']['cardinality']}
  PostgreSQL: {self.performance_metrics['4-9']['PostgreSQL']['cardinality']}

* 10-19 Joins:
  COMPASS: {self.performance_metrics['10-19']['COMPASS']['cardinality']}
  PostgreSQL: {self.performance_metrics['10-19']['PostgreSQL']['cardinality']}

* 20-28 Joins:
  COMPASS: {self.performance_metrics['20-28']['COMPASS']['cardinality']}
  PostgreSQL: {self.performance_metrics['20-28']['PostgreSQL']['cardinality']}

3. ANÁLISIS DE DISTANCIAS L1
---------------------------
Promedio de Distancias L1:
* 4-9 Joins:
  COMPASS: {self.l1_distances['4-9']['COMPASS']:.2f}
  PostgreSQL: {self.l1_distances['4-9']['PostgreSQL']:.2f}

* 10-19 Joins:
  COMPASS: {self.l1_distances['10-19']['COMPASS']:.2f}
  PostgreSQL: {self.l1_distances['10-19']['PostgreSQL']:.2f}

* 20-28 Joins:
  COMPASS: {self.l1_distances['20-28']['COMPASS']:.2f}
  PostgreSQL: {self.l1_distances['20-28']['PostgreSQL']:.2f}

4. CONCLUSIONES PRINCIPALES
--------------------------
1. COMPASS supera a PostgreSQL en el 55.75% de las queries
2. La ventaja de COMPASS es más pronunciada en queries complejas (>10 joins)
3. Las distancias L1 de COMPASS son consistentemente menores
4. COMPASS mantiene mejor control de la cardinalidad en todos los grupos
5. La diferencia en rendimiento aumenta con la complejidad de las queries

5. RECOMENDACIONES
-----------------
1. Para queries simples (4-9 joins), ambos optimizadores son competitivos
2. Para queries complejas (>10 joins), COMPASS es claramente superior
3. PostgreSQL podría beneficiarse de mejores estimaciones de cardinalidad
4. La estrategia de sketch-merging de COMPASS es particularmente efectiva
"""
        return report

# Crear instancia y generar análisis completo
analyzer = ComprehensiveJOBAnalysis()

# Generar todas las visualizaciones
winning_plot = analyzer.plot_winning_queries_comparison()
performance_plot = analyzer.plot_performance_metrics()
l1_plot = analyzer.plot_l1_distances()

# Imprimir reporte completo
print(analyzer.generate_comprehensive_report())

# Mostrar plots
plt.show()