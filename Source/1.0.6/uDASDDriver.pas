unit uDASDDriver;

{$I ..\DataAbstract.inc}


{$DEFINE MAX_SUPPORT}


interface

uses Classes, DB, uDAEngine, uDAInterfaces, uROClasses,  SDEngine,  uDAUtils,
     SDConsts, uDASQLDirUtils, uROBinaryHelpers, uDAIBInterfaces,
     uDAADOInterfaces, uDAOracleInterfaces, uDACore;

const
  stSQLBaseId = 'SQLBase';
  stOracleId = 'Oracle';
  stSQLServerId = 'SQLServer';
  stSybaseId = 'Sybase';
  stDB2Id = 'DB2';
  stInformixId = 'Informix';
  stODBCId = 'ODBC';
  stInterbaseId = 'Interbase';
  stFirebirdId = 'Firebird';
  stMySQLId = 'MySQL';
  stPostgreSQLId= 'PostgreSQL';
  stOLEDBId = 'OLEDB';


type
  TDASDDriverType = (stSQLBase,
    stOracle,
    stSQLServer,
    stSybase,
    stDB2,
    stInformix,
    stODBC,
    stInterbase,
    stFirebird,
    stMySQL,
    stPostgreSQL,
    stOLEDB);

const
  // Standard dbExpress driver identifier array (useful for lookups)
  SDDrivers: array[TDASDDriverType] of string = (
    stSQLBaseID,
    stOracleId,
    stSQLServerID,
    stSybaseID,
    stDB2Id,
    stInformixId,
    stODBCId,
    stInterbaseId,
    stFirebirdId,
    stMySQLId,
    stPostgreSQLId,
    stOLEDBID);

type
  { TDASDDriver }
  TDASDDriver = class(TDADriverReference)
  end;

  { TDAESDDriver }
  TDAESDDriver = class(TDAEDriver,IDADriver40 )
  protected
    function GetConnectionClass: TDAEConnectionClass; override;

    function GetDriverID: string; override;
    function GetDescription: string; override;

    procedure GetAuxDrivers(out List: IROStrings); override;
    procedure GetAuxParams(const AuxDriver: string; out List: IROStrings); override;

    function GetAvailableDriverOptions: TDAAvailableDriverOptions; override;
    function GetDefaultConnectionType(const AuxDriver: string): string; override; safecall;
    function GetProviderDefaultCustomParameters(Provider: string): string; safecall;
  public
  end;

  { ISDConnection
      For identification purposes. }

  ISDConnection = interface
    ['{D24A86BE-F7B1-404E-81AF-C932A8A598AC}']
    function GetDriverName: string;
    function GetDriverType: TDASDDriverType;

    property DriverName: string read GetDriverName;
    property DriverType: TDASDDriverType read GetDriverType;
  end;


  { TSDConnection }
  TSDConnection = class(TDAConnectionWrapper)
  private
    fSDDatabase: TSDDatabase;
    fSDSession: tSDSession;

  protected
    function GetConnected: Boolean; override;
    procedure SetConnected(Value: Boolean); override;

  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;

    property Database: TSDDatabase read fSDDatabase;
    property Session: tSDSession read fSDSession;

  end;

  { TDAESDConnection }
  TDAESDConnection = class(TDAEConnection, ISDConnection,IDATestableObject )
  private
    fConnection: TSDConnection;
    fDriverName: string;
    fDriverType: TDASDDriverType;

  protected
    // TDAEConnection
    function CreateCustomConnection: TCustomConnection; override;
    function CreateMacroProcessor: TDASQLMacroProcessor; override;

    function GetDatasetClass: TDAEDatasetClass; override;
    function GetStoredProcedureClass: TDAEStoredProcedureClass; override;

    procedure DoApplyConnectionString(aConnStrParser: TDAConnectionStringParser;
      aConnectionObject: TCustomConnection); override;

    function DoBeginTransaction: integer; override;
    procedure DoCommitTransaction; override;
    procedure DoRollbackTransaction; override;
    function DoGetInTransaction: boolean; override;

    function GetUserID: string; override; safecall;
    procedure SetUserID(const Value: string); override; safecall;
    function GetPassword: string; override; safecall;
    procedure SetPassword(const Value: string); override; safecall;

    procedure DoGetTableNames(out List: IROStrings); override;
    procedure DoGetStoredProcedureNames(out List: IROStrings); override;
    procedure DoGetTableFields(const aTableName: string; out Fields: TDAFieldCollection); override;

    function GetDriverName: string;
    function GetDriverType: TDASDDriverType;
    function GetSPSelectSyntax(HasArguments: Boolean): String; override; safecall;
  public
  end;


  { TDAESDQuery }
  TDAESDQuery = class(TDAEDataset, IDAMustSetParams)
  private
  protected
    function CreateDataset(aConnection: TDAEConnection): TDataset; override;
    procedure ClearParams; override;
    function DoExecute: integer; override;
    function DoGetSQL: string; override;
    procedure DoSetSQL(const Value: string); override;
    procedure DoPrepare(Value: boolean); override;
    procedure SetParamValues(AParams: TDAParamCollection); override;{$IFNDEF FPC_SAFECALL_BUG}safecall;{$ENDIF}
    procedure GetParamValues(AParams: TDAParamCollection); override;{$IFNDEF FPC_SAFECALL_BUG}safecall;{$ENDIF}

  public
  end;

  { TDASDStoredProcedure }
  TDASDStoredProcedure = class(TDAEStoredProcedure{, IDAMustSetParams})
  protected
    function CreateDataset(aConnection: TDAEConnection): TDataset; override;

    procedure RefreshParams; override;
    function GetStoredProcedureName: string; override;
    procedure SetStoredProcedureName(const Name: string); override;
    function Execute: integer; override;

    procedure SetParamValues(AParams: TDAParamCollection); override;{$IFNDEF FPC_SAFECALL_BUG}safecall;{$ENDIF}
    procedure GetParamValues(AParams: TDAParamCollection); override;{$IFNDEF FPC_SAFECALL_BUG}safecall;{$ENDIF}

  end;

procedure Register;

function SDDriverIdToSDDriverType(const anID: string): TDASDDriverType;
function GetDriverObject: IDADriver; stdcall;

implementation

uses SysUtils, INIFiles, uDADriverManager, uDARes, {uDAMacroProcessors,} Variants, SqlTimSt;

var
  _driver: TDAEDriver = nil;

procedure Register;
begin
  RegisterComponents(DAPalettePageName, [TDASDDriver]);
end;

{$IFDEF DataAbstract_SchemaModelerOnly}
{$INCLUDE ..\DataAbstract_SchemaModelerOnly.inc}
{$ENDIF DataAbstract_SchemaModelerOnly}

function GetDriverObject: IDADriver;
begin
  {$IFDEF DataAbstract_SchemaModelerOnly}
  if not RunningInSchemaModeler then begin
    result := nil;
    exit;
  end;
  {$ENDIF}

  if (_driver = nil) then _driver := TDAESDDriver.Create(nil);
  result := _driver;
end;

function SDDriverIdToSDDriverType(const anID: string): TDASDDriverType;
var
  x: TDASDDriverType;
begin
  result := stSQLBase;

  for x := Low(TDASDDriverType) to High(TDASDDriverType) do
    if SameText(SDDrivers[x], anID) then begin
      result := x;
      Exit;
    end;
end;

{ TSDConnection }
constructor TSDConnection.Create(AOwner: TComponent);
begin
  inherited;

  fSDSession := tSDSession.Create(NIL);

  fSDSession.AutoSessionName := True;
  
  fSDDatabase := TSDDatabase.Create(NIL);

  fSDDatabase.SessionName := fSDSession.SessionName;
end;

destructor TSDConnection.destroy;
begin
  inherited destroy;

  If Assigned(fSDDatabase) then fSDDatabase.Free;
  If Assigned(fSDSession) then fSDSession.Free;
end;

function TSDConnection.GetConnected: Boolean;
begin
  result := fSDDatabase.Connected;
end;

procedure TSDConnection.SetConnected(Value: Boolean);
begin
  Try
    fSDDatabase.Connected := Value;
  Except
    raise EDADriverException.Create(Trim(TuDSQLDirUtils.WindowsExceptMess));

  End;
end;

{ TDAESDConnection }

procedure TDAESDConnection.DoApplyConnectionString(
  aConnStrParser: TDAConnectionStringParser; aConnectionObject: TCustomConnection);
var
  lsAliasName: string;
  lParams: tStringList;
  I: Integer;
begin
  inherited;

  with aConnStrParser do
    begin
      lParams := tStringList.Create;

      for I := 0 to aConnStrParser.AuxParamsCount - 1 do
        lParams.Add(AuxParamNames[i] + '=' + aConnStrParser.AuxParams[AuxParamNames[i]]);

      with TSDConnection(aConnectionObject).Database do
        begin
         if AuxDriver = '' then
            raise EDADriverException.Create('No aux driver specified for SD connection');

          fDriverType := SDDriverIdToSDDriverType(AuxDriver);

          If GetName = '' then
            lsAliasName := Self.fConnection.fSDSession.SessionName
          else lsAliasName := GetName;

          TuDSQLDirUtils.SetupSDDatabase(TSDConnection(aConnectionObject).Database,
             Server,
             Database,
             lsAliasName,
             TuDSQLDirUtils.GetServerTypeAsInteger(AuxDriver),
             Userid,
             Password,
             lParams);
        end;

     lParams.Free;
  end;

end;

function TDAESDConnection.DoBeginTransaction: integer;
begin
  result := -1;

  fConnection.fSDdatabase.StartTransaction;
end;

procedure TDAESDConnection.DoCommitTransaction;
begin
  fConnection.fSDdatabase.Commit;
end;

function TDAESDConnection.CreateCustomConnection: TCustomConnection;
begin
  fConnection := TSDConnection.Create(nil);
  fConnection.fSDdatabase.LoginPrompt := FALSE;
  result := fConnection;
end;

function TDAESDConnection.GetDatasetClass: TDAEDatasetClass;
begin
  result := TDAESDQuery;
end;

function TDAESDConnection.GetStoredProcedureClass: TDAEStoredProcedureClass;
begin
  result := TDASDStoredProcedure;
end;

procedure TDAESDConnection.DoGetStoredProcedureNames(out List: IROStrings);
begin
  List := TROStrings.Create;

  fConnection.Database.GetStoredProcNames(List.Strings);
end;

procedure TDAESDConnection.DoGetTableNames(out List: IROStrings);
Var
  I: Integer;
  lsTableName: String;
begin
  List := TROStrings.Create;

  fConnection.Database.GetTableNames('', False,List.Strings);

  For I := 0 to List.Strings.Count - 1 do
    begin
      If pos(Userid + '.', List.Strings[i]) > 0 then
        begin
          lsTableName := copy(List.Strings[i], pos('.', List.Strings[i]) + 1, Length(List.Strings[i]));

          List.Strings[i] := lsTableName;
        end;
    end;
end;


procedure TDAESDConnection.DoGetTableFields(const aTableName: string; out
    Fields: TDAFieldCollection);
begin
  inherited DoGetTableFields(QuoteIdentifierIfNeeded(aTableName), Fields);
end;


procedure TDAESDConnection.DoRollbackTransaction;
begin
  fConnection.fSDdatabase.Rollback;
end;

function TDAESDConnection.DoGetInTransaction: boolean;
begin
  Result := fConnection.fSDdatabase.InTransaction;
end;

function TDAESDConnection.GetDriverName: string;
begin
  result := fDriverName
end;

function TDAESDConnection.GetDriverType: TDASDDriverType;
begin
  result := fDriverType
end;

function TDAESDConnection.CreateMacroProcessor: TDASQLMacroProcessor;
begin
  result := nil;
  case fDriverType of
    stSQLServer: result := MSSQL_CreateMacroProcessor;
    stFirebird,
    stInterbase: result := IB_CreateMacroProcessor((fDriverType= stFirebird));
    stOracle: result := Oracle_CreateMacroProcessor;
  end;
end;

function TDAESDConnection.GetPassword: string;
begin
  Result := fConnection.Database.Params.Values[szPASSWORD];
end;

function TDAESDConnection.GetUserID: string;
begin
  Result := fConnection.Database.Params.Values[szUSERNAME];
end;

procedure TDAESDConnection.SetPassword(const Value: string);
begin
  fConnection.Database.Params.Values[szPASSWORD] := Value;
end;

procedure TDAESDConnection.SetUserID(const Value: string);
begin
  fConnection.Database.Params.Values[szUSERNAME] := Value;
end;

function TDAESDConnection.GetSPSelectSyntax(
  HasArguments: Boolean): String;
begin
  case fDriverType of
    stInterbase: if HasArguments then Result := 'SELECT * FROM {0}({1})' else result := 'SELECT * FROM {0}';
    stOracle: if HasArguments then Result := 'CALL {0}({1})' else result := 'CALL {0}';
    else Result := 'EXEC {0} {1}';
  end;
end;

{ TDAESDDriver }

function TDAESDDriver.GetAvailableDriverOptions: TDAAvailableDriverOptions;
begin
  result := [doAuxDriver, doServerName, doDatabaseName, doLogin, doCustom];
end;

function  TDAESDDriver.GetDefaultConnectionType(
  const AuxDriver: string): string;
begin
  Result := inherited GetDefaultConnectionType(AuxDriver);
end;

function TDAESDDriver.GetProviderDefaultCustomParameters(Provider: string): string;
begin
  Result := '';
end;

function TDAESDDriver.GetConnectionClass: TDAEConnectionClass;
begin
  result := TDAESDConnection;
end;

function TDAESDDriver.GetDescription: string;
begin
  result := 'SQLDirect Driver';
end;

function TDAESDDriver.GetDriverID: string;
begin
  result := 'SD';
end;

procedure TDAESDDriver.GetAuxDrivers(out List: IROStrings);
var
  x: TDASDDriverType;
begin
  List := NewROStrings;

  for x := Low(TDASDDriverType) to High(TDASDDriverType) do
     List.Add(SDDrivers[x]);
end;

procedure TDAESDDriver.GetAuxParams(const AuxDriver: string;
  out List: IROStrings);
begin
  inherited;



end;


{ TDAESDQuery }
function TDAESDQuery.CreateDataset(aConnection: TDAEConnection): TDataset;
begin
  result := TSDQuery.Create(nil);

  TSDQuery(result).DatabaseName := TDAESDConnection(aConnection).fConnection.Database.DatabaseName;
  TSDQuery(result).SessionName := TDAESDConnection(aConnection).fConnection.Session.SessionName;

end;

function GetBlobValue(const val: Variant): string;
var
  lsize: integer;
  p: Pointer;
begin
  if VarType(val) = 8209 then
  begin
    lSize := VarArrayHighBound(val, 1)-VarArrayLowBound(val, 1)+1;
    p := VarArrayLock(val);
    try
      setlength(REsult, lSize);
      move(p^, Result[1], lSize);
    finally
      VarArrayUnlock(val);
    end;
  end else if vartype(val) = varEmpty then
    result := ''
  else
    result := val;
end;


function TDAESDQuery.DoExecute: integer;
begin
  TSDQuery(Dataset).ExecSQL;
  result := TSDQuery(Dataset).RowsAffected;
end;

procedure TDAESDQuery.ClearParams;
begin
  inherited;

  TSDQuery(Dataset).Params.Clear;
end;



function TDAESDQuery.DoGetSQL: string;
begin
  result := TSDQuery(Dataset).SQL.Text;
end;

procedure TDAESDQuery.DoPrepare(Value: boolean);
begin
  TSDQuery(Dataset).Prepared := Value;
end;

procedure TDAESDQuery.SetParamValues(AParams: TDAParamCollection);
begin
  SetParamValuesStd(AParams,TSDQuery(Dataset).Params);
end;

procedure TDAESDQuery.GetParamValues(AParams: TDAParamCollection);
begin
  GetParamValuesStd(AParams,TSDQuery(Dataset).Params);
end;


procedure TDAESDQuery.DoSetSQL(const Value: string);
begin
  TSDQuery(Dataset).SQL.Text := Value;
end;

{ TDASDStoredProcedure }

function TDASDStoredProcedure.CreateDataset(aConnection: TDAEConnection): TDataset;
begin
  result := TSDStoredProc.Create(nil);

  TSDStoredProc(result).DatabaseName := TDAESDConnection(aConnection).fConnection.Database.DatabaseName;
  TSDStoredProc(result).SessionName := TDAESDConnection(aConnection).fConnection.Session.SessionName;
end;

function TDASDStoredProcedure.Execute: integer;
var
  i: integer;
  _params: TDAParamCollection;
begin
  _params := GetParams;

  With TSDStoredProc(Dataset) do
    begin
      for i := 0 to (ParamCount - 1) do
        if (Params[i].ParamType in [ptInput, ptInputOutput]) then
          Params[i].Value := _params[i].Value;

       ExecProc;

       result := -1;

       for i := 0 to ParamCount - 1 do
          if (Params[i].ParamType in [ptOutput, ptInputOutput, ptResult]) then
             _params[i].Value := Params[i].Value;
    end;
end;

procedure TDASDStoredProcedure.SetParamValues(AParams: TDAParamCollection);
begin
  SetParamValuesStd(AParams, TSDStoredProc(Dataset).Params);
end;

procedure TDASDStoredProcedure.GetParamValues(AParams: TDAParamCollection);
begin
  GetParamValuesStd(AParams, TSDStoredProc(Dataset).Params);
end;


function TDASDStoredProcedure.GetStoredProcedureName: string;
begin
  result := TSDStoredProc(Dataset).StoredProcName;
end;

procedure TDASDStoredProcedure.SetStoredProcedureName(
  const Name: string);
begin
  TSDStoredProc(Dataset).StoredProcName := Name;
end;

procedure TDASDStoredProcedure.RefreshParams;
var
  FParams: TParams;
  FDAParam:  TDAParam;
  FDAParams: TDAParamCollection;
  i: integer;
  FsParamName: String;
begin
  TSDStoredProc(Dataset).Prepare;

  FParams := TSDStoredProc(Dataset).Params;
  FDAParams := GetParams;
  FDAParams.Clear;

  for i := 0 to (FParams.Count - 1) do
    begin
      FDAParam := FDAParams.Add;

      FsParamName := FParams[i].Name;
      FDAParam.Name := FsParamName;

      FDAParam.DataType := VCLTypeToDAType(FParams[i].DataType);

      FDAParam.ParamType := TDAParamType(FParams[i].ParamType);
      FDAParam.Size := FParams[i].Size;
    end;
end;

exports
  GetDriverObject name func_GetDriverObject;

initialization
  _driver := nil;
  RegisterDriverProc(GetDriverObject);
finalization
  UnregisterDriverProc(GetDriverObject);
  FreeAndNIL(_driver);
end.


