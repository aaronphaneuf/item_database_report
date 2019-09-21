import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np

#these modules are not used, but are necessary to workaround PyInstaller error
import numpy.random.common
import numpy.random.bounded_integers
import numpy.random.entropy

#declare month and year variables
current = datetime.now()
year = current.year
month = current.month
day = current.day

#dictionary for month names
months = {1:"January", 2:"February", 3:"March", 4:"April", 5:"May", 6:"June", 7:"July", 8:"August", 9:"September", 10:"October", 11:"November", 12:"December"}

#declare 1 year from today to compare zero movers in report
report_year = year-1
report_date = '{0}-{1}-{2}' .format(report_year, month, day)

#establish SQL Connection
conn = pyodbc.connect('DSN=Prototype')
cursor = conn.cursor()



#define main function
def build_idr():
    """Executes SQL Query to pull item information, writes to pandas df, and manipulates some columns to match previous IDR reports"""

    sql_query = pd.read_sql_query("""

    SELECT DISTINCT (t1.INV_ScanCode) AS UPC, t1.brd_name AS Brand, t1.inv_name AS Name, t1.inv_size AS Size, t1.dpt_name AS Department, t1.pi1_description AS Sub_Dept,
    t1.pi2_description AS Sub_Dept_Name, t9.FML_Name AS Family_Line, t1.inv_memo AS Linked_Item, t1.inv_discontinued AS Disco, t1.SIL_LastSold AS Zero_Mover, t6.inv_lastcost as AB_Cost, t6.SIB_baseprice AS AB_Retail, t6.ven_companyname AS AB_Supplier,
    t6.ord_supplierstocknumber AS AB_Supplier_Code, t6.ord_quantityinorderunit AS AB_Unit, t2.ILT_TAX_FK AS AB_Tax, t7.inv_lastcost as VIC_Cost, t7.sib_baseprice AS VIC_Retail,
    t7.ven_companyname AS VIC_Supplier, t7.ord_supplierstocknumber AS VIC_Supplier_Code, t7.ord_quantityinorderunit AS VIC_Unit, t3.ILT_TAX_FK AS Victoria_GST, t4.ILT_TAX_FK AS Victoria_PST,
    t8.inv_lastcost as PTC_Cost, t8.sib_baseprice AS PTC_Retail, t8.ven_companyname AS PTC_Supplier, t8.ord_supplierstocknumber AS PTC_Supplier_Code, t8.ord_quantityinorderunit AS PTC_Unit, t5.ILT_TAX_FK AS PTC_Tax

    FROM v_InventoryMaster as t1

    LEFT OUTER JOIN FamilyLines as t9
    ON t1.INV_FML_FK = t9.FML_PK AND t1.INV_FML_CFK = t9.FML_CPK

    LEFT OUTER JOIN StockInventoryLinkTax as t2
    on t1.INV_PK = t2.ILT_INV_FK and t1.INV_CPK = t2.ILT_INV_CFK AND t2.ILT_STO_FK = 12

    LEFT OUTER JOIN v_inventorymaster as t6
    ON t1.INV_PK = t6.INV_PK AND t1.INV_CPK = t6.INV_CPK AND t6.INV_STO_FK = 12

    LEFT OUTER JOIN StockInventoryLinkTax as t3
    on t1.INV_PK = t3.ILT_INV_FK and t1.INV_CPK = t3.ILT_INV_CFK AND t3.ILT_STO_FK = 9 AND t3.ILT_TAX_FK !=7

    LEFT OUTER JOIN v_inventorymaster as t7
    ON t1.INV_PK = t7.INV_PK AND t1.INV_CPK = t7.INV_CPK AND t7.INV_STO_FK = 9

    LEFT OUTER JOIN StockInventoryLinkTax as t4
    on t1.INV_PK = t4.ILT_INV_FK and t1.INV_CPK = t4.ILT_INV_CFK AND t4.ILT_STO_FK = 9 AND t4.ILT_TAX_FK !=2

    LEFT OUTER JOIN StockInventoryLinkTax as t5
    on t1.INV_PK = t5.ILT_INV_FK and t1.INV_CPK = t5.ILT_INV_CFK AND t5.ILT_STO_FK = 6

    LEFT OUTER JOIN v_inventorymaster as t8
    ON t1.INV_PK = t8.INV_PK AND t1.INV_CPK = t8.INV_CPK AND t8.INV_STO_FK = 6;
    """, conn)

    #write sql to dataframe
    df = pd.DataFrame(sql_query)

    #determine amount of rows
    df_length = str((len(df)))

    #fill 'not availables' with blanks
    df = df.fillna('')



    #replace number values in tax columns with named equivalents
    df['AB_Tax'].replace(2.0, 'GST', inplace=True)
    df['PTC_Tax'].replace(5.0, 'HST', inplace=True)
    df['Victoria_GST'].replace(2.0, 'GST', inplace=True)
    df['Victoria_PST'].replace(7.0, 'PST', inplace=True)

    #if item has GST and PST for Victoria, then display "GST+PST"
    df['Victoria_Tax'] = df['Victoria_GST'].fillna('') + df['Victoria_PST'].fillna('')
    df['Victoria_Tax'].replace('GSTPST', 'GST+PST', inplace=True)

    #conver zero_mover to datetime and declare a new column based off the result
    df['Zero_Mover'] = pd.to_datetime(df['Zero_Mover'])
    df['Zero_Mover'] = pd.to_datetime(df['Zero_Mover'].dt.normalize())
    df['New_Zero_Mover'] = np.where(df['Zero_Mover']> report_date, "No", "Yes")

    #delete duplicate UPCs keeping the most recently sold item
    df = df.sort_values('Zero_Mover', ascending=False).drop_duplicates('UPC').sort_index()

    #drop UPC, GST, and PST columns
    df.drop(columns=['Victoria_GST', 'Victoria_PST'])

    #drop timestamp in Last_Sold
    df['Zero_Mover'] = df['Zero_Mover'].dt.date

    #creates a new column that converts numbers to number format and includes strings
    df['UPC2'] = pd.to_numeric(df['UPC'], errors='coerce')
    df['UPC2'] = df['UPC2'].fillna(df['UPC'])
    df.drop(columns=['UPC'])

    #calculate margins
    df['AB_Margin'] = ((df['AB_Retail']-df['AB_Cost'])/df['AB_Retail'])
    df['VIC_Margin'] = ((df['VIC_Retail']-df['VIC_Cost'])/df['VIC_Retail'])
    df['PTC_Margin'] = ((df['PTC_Retail']-df['PTC_Cost'])/df['PTC_Retail'])

    #sort by sub department
    df = df.sort_values(by=['Sub_Dept'])

    #organize headers
    df = df[['UPC2', 'Brand', 'Name', 'Size', 'Department', 'Sub_Dept', 'Sub_Dept_Name', 'Family_Line', 'Linked_Item', 'Disco', 'Zero_Mover', 'New_Zero_Mover', 'AB_Cost', 'AB_Retail', 'AB_Margin', 'AB_Supplier', 'AB_Supplier_Code', 'AB_Unit', 'AB_Tax', 'VIC_Cost', 'VIC_Retail', 'VIC_Margin', 'VIC_Supplier', 'VIC_Supplier_Code', 'VIC_Unit', 'Victoria_Tax', 'PTC_Cost', 'PTC_Retail', 'PTC_Margin', 'PTC_Supplier', 'PTC_Supplier_Code', 'PTC_Unit', 'PTC_Tax']]
    #rename headers
    df.columns = ['UPC', 'Brand', 'Name', 'Size', 'Department', 'Sub Dpt', 'Sub Dpt Name', 'Family Line', 'Linked Item', 'Disco', 'Last Sold', 'Zero Mover', 'Cost', 'Retail', 'Margin', 'Supplier', 'Order Code', 'Unit', 'Tax', 'Cost', 'Retail', 'Margin', 'Supplier', 'Code', 'Unit', 'Tax', 'Cost', 'Retail', 'Margin', 'Supplier', 'Code', 'Unit', 'Tax' ]

    #write to excel
    writer = pd.ExcelWriter("{0} {1} IDR.xlsx" .format(year, months[month]), engine='xlsxwriter')
    df.to_excel(writer, sheet_name = 'IDR', startcol=0, startrow=1, index=False)

    #declare xlsx variables
    workbook = writer.book
    worksheet = writer.sheets['IDR']

    #declare second worksheet (pricing audit template)
    worksheet2 = workbook.add_worksheet('Price Audit Template')

    #set worksheet zoom level
    worksheet.set_zoom(80)
    worksheet2.set_zoom(85)

    #declare variables to match previous IDR appearance
    cell_format1 = workbook.add_format({'num_format': '0.00%', 'align' : 'center'})
    cell_format2 = workbook.add_format({'num_format' : '0 00000 00000 0', 'align' : 'center'})
    align_center = workbook.add_format({'align' : 'center'})
    align_center_audit = workbook.add_format({'border' : 1, 'align' : 'center'})
    align_center_yellow = workbook.add_format({'border' : 1, 'align' : 'center', 'bg_color': '#ffff00'})
    yellow_bg = workbook.add_format({'border' : 1, 'bg_color': '#ffff00',})
    align_center_srp = workbook.add_format({'border' : 1, 'align' : 'center', 'fg_color': '#fde9d9'})
    audit_border = workbook.add_format({'border' : 1})
    audit_percentage = workbook.add_format({'border' : 1, 'align' : 'center', 'num_format': '0.00%'})
    audit_percentage_yellow = workbook.add_format({'border' : 1, 'align' : 'center', 'num_format': '0.00%', 'bg_color': '#ffff00'})
    yellow = workbook.add_format({'bold': 1, 'border': 1, 'align': 'center', 'valign':'vcenter', 'fg_color': '#ffff00', 'text_wrap':1, 'size':12})
    srp = workbook.add_format({'bold': 1, 'border': 1, 'align': 'center', 'valign':'vcenter', 'fg_color': '#fde9d9', 'text_wrap':1, 'size':12})

    #set column widths and apply formatting, if necessary
    worksheet.set_column('A:A', 18.57, cell_format2)
    worksheet.set_column('B:B', 37.29)
    worksheet.set_column('C:C', 44.43)
    worksheet.set_column('D:D', 17.29)
    worksheet.set_column('E:E', 24.86)
    worksheet.set_column('F:F', 19.43, align_center)
    worksheet.set_column('G:G', 30.86)
    worksheet.set_column('H:H', 17.71)
    worksheet.set_column('I:I', 20.43)
    worksheet.set_column('J:J', 11, align_center)
    worksheet.set_column('K:K', 14.14, align_center)
    worksheet.set_column('L:L', 13.71, align_center)
    worksheet.set_column('M:M', 10)
    worksheet.set_column('N:N', 11.43)
    worksheet.set_column('O:O', 12, cell_format1)
    worksheet.set_column('P:P', 30.14)
    worksheet.set_column('Q:Q', 20.57)
    worksheet.set_column('R:R', 10, align_center)
    worksheet.set_column('S:S', 9.43, align_center)
    worksheet.set_column('T:T', 10)
    worksheet.set_column('U:U', 11.43)
    worksheet.set_column('V:V', 12, cell_format1)
    worksheet.set_column('W:W', 30.14)
    worksheet.set_column('X:X', 20.57)
    worksheet.set_column('Y:Y', 10, align_center)
    worksheet.set_column('Z:Z', 9.43, align_center)
    worksheet.set_column('AA:AA', 10)
    worksheet.set_column('AB:AB', 11.43)
    worksheet.set_column('AC:AC', 12, cell_format1)
    worksheet.set_column('AD:AD', 30.14)
    worksheet.set_column('AE:AE', 20.57)
    worksheet.set_column('AF:AF', 10, align_center)
    worksheet.set_column('AG:AG', 9.43, align_center)

    #filter based off length of rows
    worksheet.autofilter('A2:AG'+df_length)

    #write headers and merge
    merge_format = workbook.add_format({ 'bold': 1, 'border': 1, 'align': 'center'})
    merge_format_ab = workbook.add_format({ 'bold': 1, 'border': 1, 'align': 'center', 'fg_color': '#b1a0c7'})
    merge_format_vic = workbook.add_format({ 'bold': 1, 'border': 1, 'align': 'center', 'fg_color': '#c4d79b'})
    merge_format_ptc = workbook.add_format({ 'bold': 1, 'border': 1, 'align': 'center', 'fg_color': '#92cddc'})
    merge_format.set_font_size(14)
    merge_format_ab.set_font_size(14)
    merge_format_vic.set_font_size(14)
    merge_format_ptc.set_font_size(14)
    worksheet.merge_range('A1:L1', 'Inventory Database Report - {0} {1}' .format(months[month], year), merge_format)
    worksheet.merge_range('M1:S1', 'Alberta', merge_format_ab)
    worksheet.merge_range('T1:Z1', 'Victoria', merge_format_vic)
    worksheet.merge_range('AA1:AG1', 'Port Credit', merge_format_ptc)

    #for price audit Template
    #set column widths and apply formatting, if necessary
    worksheet2.set_row(1, 47.25)
    worksheet2.write('A1', 'Vendor:')
    worksheet2.write('A2', 'UPC/PLU#', yellow)
    worksheet2.write('B2', 'Brand', yellow)
    worksheet2.write('C2', 'Description', yellow)
    worksheet2.write('D2', 'Size/ UOM', yellow)
    worksheet2.write('E2', 'System Size', yellow)
    worksheet2.write('F2', 'Name', yellow)
    worksheet2.write('G2', 'Curr. Case Pack', yellow)
    worksheet2.write('H2', 'New Case Pack', yellow)
    worksheet2.write('I2', 'Curr Case Cost', yellow)
    worksheet2.write('J2', 'New Case Cost', yellow)
    worksheet2.write('K2', 'Curr. Unit Retail', yellow)
    worksheet2.write('L2', 'New Unit Retail', yellow)
    worksheet2.write('M2', 'SRP', srp)
    worksheet2.write('N2', 'Curr. GM%', yellow)
    worksheet2.write('O2', 'New GM%', yellow)
    worksheet2.write('P2', 'Curr. Supplier', yellow)
    worksheet2.write('Q2', 'New Supplier', yellow)
    worksheet2.write('R2', 'Curr. Item/Order Code #', yellow)
    worksheet2.write('S2', 'New Item/Order Code #', yellow)
    worksheet2.write('T2', 'Curr Subdept #', yellow)
    worksheet2.write('U2', 'New Subdept #', yellow)
    worksheet2.write('V2', 'Curr Subdept Name', yellow)
    worksheet2.write('W2', 'New Subdept Name', yellow)
    worksheet2.write('X2', 'Curr Tax Status', yellow)
    worksheet2.write('Y2', 'New Tax Status', yellow)
    worksheet2.set_column('A:A', 14.29)
    worksheet2.set_column('B:B', 31.43)
    worksheet2.set_column('C:C', 38)
    worksheet2.set_column('D:D', 8)
    worksheet2.set_column('E:E', 14.71)
    worksheet2.set_column('F:F', 42.57)
    worksheet2.set_column('G:G', 12, align_center)
    worksheet2.set_column('H:H', 8.86, align_center)
    worksheet2.set_column('I:I', 8.86)
    worksheet2.set_column('J:J', 8.86)
    worksheet2.set_column('K:K', 10)
    worksheet2.set_column('L:L', 8.86)
    worksheet2.set_column('M:M', 8.86)
    worksheet2.set_column('N:N', 11.29,)
    worksheet2.set_column('O:O', 10.57,)
    worksheet2.set_column('P:P', 26.86)
    worksheet2.set_column('Q:Q', 13.86)
    worksheet2.set_column('R:R', 18.14)
    worksheet2.set_column('S:S', 11.43)
    worksheet2.set_column('T:T', 16.29)
    worksheet.set_column('U:U', 10)
    worksheet2.set_column('V:V', 19.57)
    worksheet2.set_column('W:W', 15.57)
    worksheet2.set_column('X:X', 12)
    worksheet2.set_column('Y:Y', 14.71)

    #for loops for all the formulas / highlights

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 0, '', audit_border)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0},IDR!$A:$B,2,FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 1, formula, audit_border)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 2, '', audit_border)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 3, '', audit_border)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0},IDR!$A:$D,4,FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 4, formula, audit_border)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0},IDR!$A:$C,3,FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 5, formula, audit_border)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0},IDR!$A:$R,18,FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 6, formula, align_center_audit)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 7, '', align_center_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0},IDR!$A:$M,13,FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 8, formula, align_center_audit)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 9, '', align_center_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0},IDR!$A:$N,14,FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 10, formula, align_center_audit)

    for row_num in range(3, 301):
        formula = '=(K{0})' .format(row_num)
        worksheet2.write(row_num-1, 11, formula, align_center_yellow)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 12, '', align_center_srp)

    for row_num in range(3, 301):
        formula = '=SUM((K{0}-I{0})/K{0})' .format(row_num)
        worksheet2.write(row_num-1, 13, formula, audit_percentage)

    for row_num in range(3, 301):
        formula = '=SUM((L{0}-J{0})/L{0})' .format(row_num)
        worksheet2.write(row_num-1, 14, formula, audit_percentage_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0}, IDR!$A:$P, 16, FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 15, formula, audit_border)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 16, '', align_center_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0}, IDR!$A:$Q, 17, FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 17, formula, audit_border)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 18, '', align_center_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0}, IDR!$A:$F, 6, FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 19, formula, align_center_audit)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 20, '', align_center_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0}, IDR!$A:$G, 7, FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 21, formula, align_center_audit)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 22, '', align_center_yellow)

    for row_num in range(3, 301):
        formula = '=VLOOKUP(A{0}, IDR!$A:$S, 19, FALSE)' .format(row_num)
        worksheet2.write(row_num-1, 23, formula, align_center_audit)

    for row_num in range(3, 301):
        worksheet2.write(row_num-1, 24, '', align_center_yellow)

        #save file
    writer.save()

print('Building IDR...')
build_idr()
