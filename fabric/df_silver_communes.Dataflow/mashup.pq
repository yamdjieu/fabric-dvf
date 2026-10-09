[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared communes = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "4bbeb02e-f4d8-434b-b238-91beaf6d15c4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "936e953a-ecc6-43a8-9bcf-ea9f13c4a0f9"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 4" = #"Navigation 3"{[Name = "bronze"]}[Content],
  #"Navigation 5" = #"Navigation 4"{[Name = "communes"]}[Content],
  Navigation = #"Navigation 5"{[Name = "communes.json"]}[Content],
  #"Lecture JSON Lines" = Table.FromList(List.Select(Lines.FromBinary(Navigation, null, null, 65001), each Text.Trim(_) <> ""), each {Json.Document(_)}, {"commune"}),
  #"commune développé" = Table.ExpandRecordColumn(#"Lecture JSON Lines", "commune", {"nom", "code", "codesPostaux", "population", "departement", "region"}, {"nom", "code", "codesPostaux", "population", "departement", "region"}),
  #"departement développé" = Table.ExpandRecordColumn(#"commune développé", "departement", {"code", "nom"}, {"departement.code", "departement.nom"}),
  #"region développé" = Table.ExpandRecordColumn(#"departement développé", "region", {"code", "nom"}, {"region.code", "region.nom"}),
  #"Colonne personnalisée ajoutée" = Table.AddColumn(#"region développé", "code_postal", each List.First([codesPostaux])),
  #"Colonnes supprimées" = Table.RemoveColumns(#"Colonne personnalisée ajoutée", {"codesPostaux"}),
  #"Colonnes renommées" = Table.RenameColumns(#"Colonnes supprimées", {{"nom", "nom_commune"}, {"code", "code_commune"}, {"departement.code", "code_departement"}, {"departement.nom", "nom_departement"}, {"region.code", "code_region"}, {"region.nom", "nom_region"}}),
  #"Type de colonne changé" = Table.TransformColumnTypes(#"Colonnes renommées", {{"nom_commune", type text}, {"code_commune", type text}, {"code_departement", type text}, {"nom_departement", type text}, {"code_region", type text}, {"nom_region", type text}, {"code_postal", type text}, {"population", Int64.Type}})
in
  #"Type de colonne changé";
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  #"Navigation 1" = Source{[workspaceId = "4bbeb02e-f4d8-434b-b238-91beaf6d15c4"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "936e953a-ecc6-43a8-9bcf-ea9f13c4a0f9"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "silver", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
