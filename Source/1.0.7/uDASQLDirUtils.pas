unit uDASQLDirUtils;

interface

Uses SDEngine, SysUtils, SDConsts, Classes;

type
  TuDSQLDirUtils = class
  public
    class function GetServerTypeAsString(ServerType: TSDServerType): String;
    class function GetServerTypeAsInteger(ServerType: String): Integer;
    class function GetServerTypeAsSdServerType(ServerType: String): tSDserverType;
    class function WindowsExceptMess: String;

    class procedure SetupSDDatabase(Const ADatabase: tsddatabase;
                  AServerName: String;
                  ARemoteDatabase: String;
                  AAliasName: String;
                  AServerType: Integer;
                  AUserName,
                  APassword: String;
                  AParams: tStringList;
                  ASQLLibrary: String);
  end;




implementation


class procedure TuDSQLDirUtils.SetupSDDatabase;
begin
  ADatabase.Connected := False;

  ADatabase.LoginPrompt := False;

  ADatabase.KeepConnection := True;

  If ADatabase.DatabaseName <> AAliasName then
    ADatabase.DatabaseName := AAliasName;

  ADatabase.ServerType := TSDServerType(AServerType);

  ADatabase.Params.Clear;

  If TSDServerType(AServerType) <> stOLEDB then
    begin
      ADatabase.Params.Values[szUSERNAME] := AUserName;
      ADatabase.Params.Values[szPASSWORD] := APassword;
    end;

  If TSDServerType(AServerType) = stOLEDB then
    begin
       ADatabase.Remotedatabase := 'Provider=' + aSQLLibrary + ';' +
      'User ID=' + aUsername + ';' +
      'Initial Catalog=' + aDatabase + ';' +
      'Data Source=' + aServername + ';';

    end
  else
  If TSDServerType(AServerType) = stSQLServer then
    begin
      ADatabase.Params.Add('USE OLEDB=TRUE');
      ADatabase.Params.Add('INIT COM=FALSE');

      ADatabase.Remotedatabase := AServerName + ':' + ARemoteDatabase;
    end
  else
  If (TSDServerType(AServerType) = stFirebird) or
     (TSDServerType(AServerType) = stInterbase) then
    begin
      ADatabase.Params.Add('SQL Dialect=3');

      If Trim(AServername) = '' then
        ADatabase.Remotedatabase := ARemoteDatabase
      else
      If Uppercase(Trim(AServername)) = 'LOCALHOST' then
        ADatabase.Remotedatabase := ARemoteDatabase
      else
        ADatabase.Remotedatabase := AServerName + ':' + ARemoteDatabase;

     If (TSDServerType(AServerType) = stFirebird) then
        ADatabase.Params.Add('Firebird API Library=' + aSQLLibrary);
    end
  else
    begin
      ADatabase.Remotedatabase := AServerName + ':' + ARemoteDatabase;
    end;

  if Assigned(AParams) then
    ADatabase.Params.Add(AParams.Text);
end;

class function TuDSQLDirUtils.GetServerTypeAsString;
begin
  case ServerType of
   stSQLBase: Result := 'SQLBase';
   stOracle: Result := 'Oracle';
   stSQLServer: Result := 'SQLServer';
   stSybase: Result := 'Sybase';
   stDB2: Result := 'DB2';
   stInformix: Result := 'Informix';
   stODBC: Result := 'ODBC';
   stInterbase: result := 'Interbase';
   stFirebird: result := 'Firebird';
   stMySQL: result := 'MySQL';
   stPostgreSQL: result := 'PostgreSQL';
   stOLEDB: result := 'OLEDB';
  else
    Result := '';
  end;
end;

class function TuDSQLDirUtils.GetServerTypeAsSdServerType(ServerType: String): tSDserverType;
begin
  Result := tSDServerType(GetServerTypeAsInteger(ServerType));
end;

class function TuDSQLDirUtils.GetServerTypeAsInteger;
begin
  Result := -1;

  ServerType := uppercase(serverType);

  if ServerType ='SQLBASE' then
    Result := integer(stSQLBase)
  else
  if Servertype = 'ORACLE' then
    Result := integer(stOracle)
  else
  if ServerType = 'SQLSERVER' then
    Result := integer(stSQLServer)
  else
  If Servertype = 'SYBASE' then
    Result := integer(stSybase)
  else
  if Servertype = 'DB2' then
    result := integer(stDB2)
  else
  if Servertype = 'INFORMIX' then
    result := integer(stInformix)
  else
  if Servertype = 'ODBC' then
    result := integer(stODBC)
  else
  if Servertype = ' INTERBASE' then
    result := integer(stInterbase)
  else
  if Servertype = 'FIREBIRD' then
    result := integer(stFirebird)
  else
  if Servertype = 'MYSQL' then
    result := integer(stMySQL)
  else
  if Servertype = 'POSTGRESQL' then
    result := integer(stPostgreSQL)
  else
  if Servertype = 'OLEDB' then
    result := integer(stOLEDB);
end;

class function TuDSQLDirUtils.WindowsExceptMess;
Var
  ValSize: Integer;
  P: Pointer;
  S: String;
begin
  Result := '';

  If ExceptObject = NIL then Exit;

  ValSize := 255;

  P := AllocMem(ValSize);

  ExceptionErrorMessage(ExceptObject, ExceptAddr, P,  ValSize);

  {$IFDEF UNICODE}
  S := StrPas(PWideChar(P));
  {$ELSE}
  S := StrPas(P);
  {$ENDIF}
  

  FreeMem(P);

  S := Copy(S, (Pos('.', S) + 1), Length(S) - Pos('.', S));
  Result := Copy(S, (Pos('.', S) + 1), Length(S) - Pos('.', S));
end;


end.

