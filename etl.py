import xml.etree.ElementTree as ET
import pandas as pd
import os
import glob

def main():
    # Define paths
    DATA_DIR = 'datos'
    
    # Lists to store data
    athletes_data = []
    clubs_data = []
    competitions_data = []
    results_data = []

    # Helper to safe get attribute
    def get_attr(element, attr, default=None):
        if element is None:
            return default
        return element.attrib.get(attr, default)

    # Get all XML files
    xml_files = glob.glob(os.path.join(DATA_DIR, '*.xml'))
    print(f"Encontrados {len(xml_files)} archivos XML.")

    comp_counter = 1

    for xml_file in xml_files:
        print(f"Procesando {xml_file}...")
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # LENEX structure: LENEX -> MEETS -> MEET
            meets = root.findall('MEETS/MEET')
            
            for meet in meets:
                # --- Competition Data ---
                comp_id = comp_counter
                comp_counter += 1
                
                comp_name = get_attr(meet, 'name')
                comp_city = get_attr(meet, 'city')
                comp_nation = get_attr(meet, 'nation')
                comp_course = get_attr(meet, 'course')
                comp_timing = get_attr(meet, 'timing')
                
                # Pool info
                pool = meet.find('POOL')
                comp_lanes = get_attr(pool, 'lanemax') if pool is not None else None
                
                # Dates from Sessions
                sessions = meet.findall('SESSIONS/SESSION')
                dates = [get_attr(s, 'date') for s in sessions if get_attr(s, 'date')]
                if dates:
                    comp_start = min(dates)
                    comp_end = max(dates)
                else:
                    comp_start = None
                    comp_end = None
                    
                competitions_data.append({
                    'ID': comp_id,
                    'nombre': comp_name,
                    'ciudad': comp_city,
                    'tipo_piscina': comp_course,
                    'fecha_inicio': comp_start,
                    'fecha_fin': comp_end,
                    'pais': comp_nation,
                    'cronometraje': comp_timing,
                    'numeroCalles': comp_lanes
                })
                
                # --- Events Mapping ---
                # eventid -> {distance, stroke, round, relaycount, date, time}
                events_map = {}
                heats_map = {} # heatid -> {date, time}

                for session in sessions:
                    session_date = get_attr(session, 'date')
                    events = session.findall('EVENTS/EVENT')
                    for event in events:
                        event_id = get_attr(event, 'eventid')
                        event_round = get_attr(event, 'round')
                        event_time = get_attr(event, 'daytime')
                        
                        swimstyle = event.find('SWIMSTYLE')
                        if swimstyle is not None:
                            distance = get_attr(swimstyle, 'distance')
                            stroke = get_attr(swimstyle, 'stroke')
                            relaycount = int(get_attr(swimstyle, 'relaycount', '1'))
                        else:
                            distance = None
                            stroke = None
                            relaycount = 1
                            
                        events_map[event_id] = {
                            'distance': distance,
                            'stroke': stroke,
                            'round': event_round,
                            'relaycount': relaycount,
                            'date': session_date,
                            'time': event_time
                        }

                        # Process Heats
                        heats = event.findall('HEATS/HEAT')
                        for heat in heats:
                            heat_id = get_attr(heat, 'heatid')
                            heat_time = get_attr(heat, 'daytime')
                            if heat_id:
                                heats_map[heat_id] = {
                                    'date': session_date,
                                    'time': heat_time
                                }
                
                # --- Clubs and Athletes ---
                clubs = meet.findall('CLUBS/CLUB')
                for club in clubs:
                    club_code = get_attr(club, 'code')
                    club_name = get_attr(club, 'name')
                    club_nation = get_attr(club, 'nation')
                    
                    clubs_data.append({
                        'club_code': club_code,
                        'club_name': club_name,
                        'club_nation': club_nation
                    })
                    
                    athletes = club.findall('ATHLETES/ATHLETE')
                    for athlete in athletes:
                        athlete_id = get_attr(athlete, 'athleteid')
                        
                        athletes_data.append({
                            'ID': athlete_id,
                            'NOMBRE': get_attr(athlete, 'firstname'),
                            'APELLIDOS': get_attr(athlete, 'lastname'),
                            'birthday': get_attr(athlete, 'birthdate'),
                            'género': get_attr(athlete, 'gender')
                        })
                        
                        # --- Results ---
                        results = athlete.findall('RESULTS/RESULT')
                        for result in results:
                            event_id = get_attr(result, 'eventid')
                            heat_id = get_attr(result, 'heatid')
                            
                            event_info = events_map.get(event_id)
                            
                            if not event_info:
                                continue
                                
                            if event_info['relaycount'] > 1:
                                continue # Skip relays

                            # Determine date and time
                            # Try heat info first
                            race_date = None
                            race_time = None
                            
                            if heat_id and heat_id in heats_map:
                                race_date = heats_map[heat_id]['date']
                                race_time = heats_map[heat_id]['time']
                            else:
                                # Fallback to event info
                                race_date = event_info['date']
                                race_time = event_info['time']
                                
                            # Result info
                            swimtime = get_attr(result, 'swimtime')
                            status = get_attr(result, 'status') # DSQ, DNS, etc.
                            points = get_attr(result, 'points')
                            
                            # Determine "descalificado?"
                            # If status is present, it's likely a disqualification or non-start
                            is_dsq = status if status else "No"
                            
                            splits = result.findall('SPLITS/SPLIT')
                            
                            if splits:
                                for split in splits:
                                    results_data.append({
                                        'id_competicion': comp_id,
                                        'id_atleta': athlete_id,
                                        'club_code': club_code,
                                        'distancia': event_info['distance'],
                                        'estilo': event_info['stroke'],
                                        'ronda': event_info['round'],
                                        'tiempo_final': swimtime,
                                        'descalificado?': is_dsq,
                                        'puntos': points,
                                        'distancia_parcial': get_attr(split, 'distance'),
                                        'tiempo_acumulado': get_attr(split, 'swimtime'),
                                        'fecha': race_date,
                                        'hora': race_time
                                    })
                            else:
                                # No splits (maybe 50m race or DSQ/DNS)
                                # Add one row with NA for split info
                                results_data.append({
                                        'id_competicion': comp_id,
                                        'id_atleta': athlete_id,
                                        'club_code': club_code,
                                        'distancia': event_info['distance'],
                                        'estilo': event_info['stroke'],
                                        'ronda': event_info['round'],
                                        'tiempo_final': swimtime,
                                        'descalificado?': is_dsq,
                                        'puntos': points,
                                        'distancia_parcial': None, # NA
                                        'tiempo_acumulado': None, # NA
                                        'fecha': race_date,
                                        'hora': race_time
                                    })

        except Exception as e:
            print(f"Error procesando {xml_file}: {e}")

    # Create DataFrames
    print("Generando DataFrames...")
    df_athletes = pd.DataFrame(athletes_data).drop_duplicates(subset=['ID'])
    df_clubs = pd.DataFrame(clubs_data).drop_duplicates(subset=['club_code'])
    df_competitions = pd.DataFrame(competitions_data)
    df_results = pd.DataFrame(results_data)

    # Save to CSV
    print("Guardando CSVs...")
    df_athletes.to_csv('atletas.csv', index=False, encoding='utf-8')
    df_clubs.to_csv('clubes.csv', index=False, encoding='utf-8')
    df_competitions.to_csv('competiciones.csv', index=False, encoding='utf-8')
    df_results.to_csv('resultados.csv', index=False, encoding='utf-8')

    print("Proceso completado. Archivos CSV generados:")
    print("- atletas.csv")
    print("- clubes.csv")
    print("- competiciones.csv")
    print("- resultados.csv")

if __name__ == "__main__":
    main()
