Attribute VB_Name = "ExportCSVs"
Option Compare Database

Public Sub ExportAllTables_to_CSV()

    Dim strFolder As String
    strFolder = Application.CurrentProject.Path & "\"

    Dim obj As AccessObject, dbs As Object
    Set dbs = Application.CurrentData

    For Each obj In dbs.AllTables

        If Left(obj.Name, 4) <> "MSys" Then
        
            DoCmd.TransferText acExportDelim, , obj.Name, strFolder & obj.Name & ".csv", True
            
        End If

    Next obj

End Sub
